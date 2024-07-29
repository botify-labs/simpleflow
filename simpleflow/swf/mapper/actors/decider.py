from __future__ import annotations

from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError, ReadTimeoutError

from simpleflow import format, logging_context
from simpleflow.swf.mapper.actors.core import Actor
from simpleflow.swf.mapper.exceptions import (
    DoesNotExistError,
    PollTimeout,
    ResponseError,
    extract_error_code,
    extract_message,
)
from simpleflow.swf.mapper.models.decision.base import Decision
from simpleflow.swf.mapper.models.history.base import History
from simpleflow.swf.mapper.models.workflow import WorkflowExecution, WorkflowType
from simpleflow.swf.mapper.responses import Response
from simpleflow.utils import json_dumps

if TYPE_CHECKING:
    from simpleflow.swf.mapper.models.domain import Domain


class Decider(Actor):
    """Decider actor implementation

    :param  domain: Domain the Actor should interact with
    :param  task_list: task list the Actor should watch for tasks on
    """

    def __init__(self, domain: Domain, task_list: str) -> None:
        super().__init__(domain, task_list)

    def complete(
        self, task_token: str, decisions: list[Decision] | None = None, execution_context: str | Any | None = None
    ):
        """Responds to ``swf`` decisions have been made about
        the task with `task_token``

        :param  task_token: completed decision task token

        :param  decisions: The list of decisions (possibly empty)
                           made by the decider while processing this decision task
        :type   decisions: list[simpleflow.swf.mapper.models.decision.base.Decision]
        :param execution_context: User-defined context to add to workflow execution.
        """
        if execution_context is not None and not isinstance(execution_context, str):
            execution_context = json_dumps(execution_context)
        try:
            self.respond_decision_task_completed(
                task_token,
                decisions,
                execution_context=format.execution_context(execution_context),
            )
        except ClientError as e:
            error_code = extract_error_code(e)
            message = extract_message(e)
            if error_code == "UnknownResourceFault":
                raise DoesNotExistError(
                    f"Unable to complete decision task with token={task_token}",
                    message,
                ) from e
            raise ResponseError(message, error_code=error_code) from e
        finally:
            logging_context.reset()

    def poll(self, task_list: str | None = None, identity: str | None = None, **kwargs) -> Response:
        """
        Polls a decision task and returns the token and the full history of the
        workflow's events.

        :param task_list: task list to poll for decision tasks from.
        :param identity: Identity of the decider making the request,
        which is recorded in the DecisionTaskStarted event in the
        workflow history.

        :returns: a Response object with history, token, and execution set
        """
        logging_context.reset()
        task_list = task_list or self.task_list

        try:
            task = self.poll_for_decision_task(
                self.domain.name,
                task_list=task_list,
                identity=format.identity(identity),
                **kwargs,
            )
        except ReadTimeoutError:
            task = {}

        token = task.get("taskToken")
        if not token:
            raise PollTimeout("Decider poll timed out")

        events = task["events"]
        logging_context.set("workflow_id", task["workflowExecution"]["workflowId"])
        logging_context.set("task_type", "decision")
        logging_context.set("event_id", task["startedEventId"])

        next_page = task.get("nextPageToken")
        while next_page:
            try:
                task = self.poll_for_decision_task(
                    self.domain.name,
                    task_list=task_list,
                    identity=format.identity(identity),
                    next_page_token=next_page,
                    **kwargs,
                )
            except ClientError as e:
                error_code = extract_error_code(e)
                message = extract_message(e)
                if error_code == "UnknownResourceFault":
                    raise DoesNotExistError(
                        "Unable to poll decision task",
                        message,
                    ) from e

                raise ResponseError(message, error_code=error_code) from e

            token = task.get("taskToken")
            if not token:
                raise PollTimeout("Decider poll timed out")

            events.extend(task["events"])
            next_page = task.get("nextPageToken")

        history = History.from_event_list(events)

        workflow_type = WorkflowType(
            domain=self.domain,
            name=task["workflowType"]["name"],
            version=task["workflowType"]["version"],
        )
        execution = WorkflowExecution(
            domain=self.domain,
            workflow_id=task["workflowExecution"]["workflowId"],
            run_id=task["workflowExecution"]["runId"],
            workflow_type=workflow_type,
        )

        # TODO: move history into execution (needs refactoring on WorkflowExecution.history())
        return Response(token=token, history=history, execution=execution)
