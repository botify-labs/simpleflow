from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

import typer
from typing_extensions import Annotated

from simpleflow import Workflow, format
from simpleflow.command import get_progression_callback, get_workflow_type, with_format
from simpleflow.history import History
from simpleflow.swf import helpers
from simpleflow.swf.mapper.models import WorkflowExecution
from simpleflow.swf.utils import set_workflow_class_name
from simpleflow.utils import import_from_module, json_dumps


class Status(str, Enum):
    open = "open"
    closed = "closed"


class CloseStatus(str, Enum):
    completed = "completed"
    failed = "failed"
    canceled = "canceled"
    terminated = "terminated"
    continued_as_new = "continued_as_new"


class OutputFormat(str, Enum):
    events = "events"
    raw = "raw"
    cooked = "cooked"
    cooked_alt = "cooked_alt"


app = typer.Typer(no_args_is_help=True)

TIMESTAMP_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
]


@app.command()
def filter(
    ctx: typer.Context,
    domain: Annotated[str, typer.Argument(envvar="SWF_DOMAIN")],
    status: Annotated[Status, typer.Option("--status", "-s")] = Status.open,
    tag: str | None = None,
    workflow_id: str | None = None,
    workflow_type: str | None = None,
    workflow_type_version: str | None = None,
    close_status: CloseStatus | None = None,
    started_since: int = 1,
    from_date: Annotated[datetime, typer.Option(formats=TIMESTAMP_FORMATS)] | None = None,
    to_date: Annotated[datetime, typer.Option(formats=TIMESTAMP_FORMATS)] | None = None,
):
    """
    Filter workflow executions.
    """
    status = status.upper()
    kwargs: dict[str, Any] = {}
    if status == WorkflowExecution.STATUS_OPEN:
        if from_date:
            kwargs["oldest_date"] = from_date
            kwargs["latest_date"] = to_date
        else:
            kwargs["oldest_date"] = started_since
    else:
        if from_date:
            kwargs["start_oldest_date"] = from_date
            kwargs["start_latest_date"] = to_date
        else:
            kwargs["start_oldest_date"] = started_since

    if close_status and status != WorkflowExecution.STATUS_CLOSED:
        raise Exception("Closed status not supported for non-closed workflows.")
    elif close_status:
        kwargs["close_status"] = close_status.upper()

    print(
        with_format(ctx.parent)(helpers.filter_workflow_executions)(
            domain,
            status=status,
            tag=tag,
            workflow_id=workflow_id,
            workflow_type_name=workflow_type,
            workflow_type_version=workflow_type_version,
            callback=get_progression_callback("executionInfos"),
            **kwargs,
        )
    )


@app.command()
def start(
    ctx: typer.Context,
    workflow: str,
    domain: Annotated[str, typer.Option(envvar="SWF_DOMAIN")],
    input: Annotated[str, typer.Option("--input", "-i", help="input JSON")] | None = None,
):
    """
    Start a workflow.
    """
    workflow_class: type[Workflow] = import_from_module(workflow)
    wf_input: dict[str, Any] = {}
    if input is not None:
        json_input = format.decode(input)
        if isinstance(json_input, list):
            wf_input = {"args": json_input, "kwargs": {}}
        elif isinstance(json_input, dict) and ("args" not in json_input or "kwargs" not in json_input):
            wf_input = {"args": [], "kwargs": json_input}
        else:
            wf_input = json_input
    workflow_type = get_workflow_type(domain, workflow_class)
    set_workflow_class_name(wf_input, workflow_class)
    get_task_list = getattr(workflow_class, "get_task_list", None)
    if get_task_list:
        if not callable(get_task_list):
            raise Exception("get_task_list must be a callable")
        if isinstance(wf_input, dict):
            args = wf_input.get("args", [])
            kwargs = wf_input.get("kwargs", {})
        else:
            args = []
            kwargs = wf_input
        task_list = get_task_list(workflow_class, *args, **kwargs)
    else:
        task_list = workflow_class.task_list
    execution = workflow_type.start_execution(
        # workflow_id=workflow_id,
        task_list=task_list,
        # execution_timeout=execution_timeout,
        input=wf_input,
        # tag_list=tags,
        # decision_tasks_timeout=decision_tasks_timeout,
    )

    def get_infos():
        return ["workflow_id", "run_id"], [[execution.workflow_id, execution.run_id]]

    print(with_format(ctx.parent)(get_infos)())


_NOTSET = object()


@app.command()
def history(
    ctx: typer.Context,
    domain: Annotated[str, typer.Option(envvar="SWF_DOMAIN")],
    workflow_id: str,
    run_id: str | None = None,
    output_format: Annotated[OutputFormat, typer.Option("--output-format", "--of")] = OutputFormat.events,
    reverse_order: bool = False,
):
    # print(ctx)
    # format = ctx.parent.parent.params.get("format")
    # print(format)
    from simpleflow.swf.mapper.models.history.base import History as BaseHistory

    ex = helpers.get_workflow_execution(domain, workflow_id, run_id)
    if not ex:
        print(f"Execution {workflow_id} {run_id} not found" if run_id else f"Workflow {workflow_id} not found")
        ctx.exit(1)
    events = ex.history_events(
        callback=get_progression_callback("events"),
        reverse_order=reverse_order,
    )
    if output_format == OutputFormat.events:
        pass
    else:
        raw_history = BaseHistory.from_event_list(events)
        history = History(raw_history)
        if output_format == OutputFormat.raw:
            events = []
            for event in history.events:
                e = {}
                for k in ["id", "type", "state", "timestamp", "input", "control", *event.__dict__]:
                    if k.startswith("_") or k == "raw":
                        continue
                    v = getattr(event, k, _NOTSET)
                    if v is _NOTSET:
                        continue
                    e[k] = v
                events.append(e)
        elif output_format == OutputFormat.cooked:
            history.parse()
            events = {
                "workflow": history.workflow,
                "activities": history.activities,
                "child_workflows": history.child_workflows,
                "markers": history.markers,
                "timers": history.timers,
                "signals": history.signals,
                "signal_lists": history.signal_lists,
                "external_workflows_signaling": history.external_workflows_signaling,
                "signaled_workflows": history.signaled_workflows,
            }
        elif output_format == OutputFormat.cooked_alt:
            history.parse()
            events = {
                "workflow": [t for t in history.tasks if t.type == "child_workflow"],
                "activities": [t for t in history.tasks if t.type == "activity"],
                "child_workflows": history.child_workflows,
                "markers": history.markers,
                "timers": history.timers,
                "signals": [t for t in history.tasks if t.type == "signal"],
                "signal_lists": history.signal_lists,
                "external_workflows_signaling": history.external_workflows_signaling,
                "signaled_workflows": history.signaled_workflows,
            }
        else:
            raise NotImplementedError
    print(json_dumps(events))


if __name__ == "__main__":
    # from click.core import Command
    #
    # parent = typer.Context(command=Command(name="main"))
    # parent.params["format"] = "json"
    # filter(
    #     ctx=typer.Context(
    #         command=Command(name="filter"), parent=typer.Context(command=Command(name="main"), parent=parent)
    #     ),
    #     domain="TestDomain",
    # )
    app()
