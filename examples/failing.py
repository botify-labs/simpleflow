from __future__ import annotations

import time
from typing import TYPE_CHECKING

from simpleflow import Workflow, activity, futures
from simpleflow.canvas import Group
from simpleflow.constants import MINUTE
from simpleflow.log import END, GREEN, ORANGE, RED, YELLOW
from simpleflow.swf.task import WorkflowTask
from simpleflow.task import TaskFailureContext

if TYPE_CHECKING:
    pass


@activity.with_attributes(task_list="quickstart", version="example", retry=1, raises_on_failure=False)
def fail_but_dont_raise():
    raise ValueError("This task had a problem but it's okay, YOU SHOULD NOT SEE THIS")


@activity.with_attributes(task_list="quickstart", version="example", raises_on_failure=True)
def fail_and_raise():
    raise ValueError("This task had a problem and it will fail the workflow! (this is normal if you see this)")


@activity.with_attributes(
    task_list="quickstart",
    version="example",
    start_to_close_timeout=10,
    raises_on_failure=False,
)
def timeout_no_raise():
    time.sleep(120)


def colorize(color, message):
    return "".join([color, message, END])


class FailingWorkflow(Workflow):
    name = "failing"
    version = "example"
    task_list = "example"
    retry = 1

    def run(self, fail=True):
        x = self.submit(fail_but_dont_raise)
        if fail:
            y = self.submit(fail_and_raise)
            futures.wait(x, y)
            raise ValueError("YOU SHOULD NEVER SEE THIS")
        else:
            futures.wait(x)

    def on_task_failure(self, failure_context: TaskFailureContext) -> TaskFailureContext | None:
        print(
            colorize(
                YELLOW,
                "FailingWorkflow.on_task_failure: {}: {!r} (started_id: {})".format(
                    failure_context.task_name,
                    failure_context.exception,
                    failure_context.event.get("started_id"),
                ),
            )
        )
        return None  # no specific handling


class NotFailingWorkflow(Workflow):
    name = "basic"
    version = "example"
    task_list = "example"

    def run(self, *args, **kwargs):
        print(colorize(GREEN, f"NotFailingWorkflow args: {args}"))
        print(colorize(GREEN, f"NotFailingWorkflow kwargs: {kwargs}"))
        g = Group(raises_on_failure=False)
        g.append(FailingWorkflow)
        g.append(timeout_no_raise)
        f = self.submit(g)
        futures.wait(f)

    def on_failure(self, history, reason, details=None):
        print(colorize(RED, "NotFailingWorkflow.on_failure called, it shouldn't :'("))

    def on_completed(self, history):
        print(colorize(GREEN, "NotFailingWorkflow: workflow completed!"))

    def on_task_failure(self, failure_context: TaskFailureContext) -> TaskFailureContext | None:
        # print(failure_context)
        if isinstance(failure_context.a_task, WorkflowTask) and failure_context.task_name == "failing":
            print(
                colorize(
                    GREEN,
                    "NotFailingWorkflow.on_task_failure: {}: {!r}: retry_count={}".format(
                        failure_context.task_name,
                        failure_context.exception,
                        failure_context.retry_count,
                    ),
                )
            )
            if failure_context.retry_count < 2:  # maximum 2 retries
                failure_context.decision = failure_context.Decision.retry_later
                failure_context.retry_wait_timeout = 1 * MINUTE
                if failure_context.retry_count == 1:  # don't fail on this retry
                    failure_context.a_task.kwargs = {"fail": False}
                return failure_context
        else:
            print(
                colorize(
                    ORANGE,
                    "===> unhandled: {}, {!r}".format(
                        failure_context.task_name,
                        failure_context.exception,
                    ),
                )
            )
        return None
