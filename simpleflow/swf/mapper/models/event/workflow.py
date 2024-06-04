from __future__ import annotations

import sys

if sys.version_info < (3, 8):
    from typing_extensions import TypedDict
else:
    from typing import ClassVar, TypedDict

from simpleflow.swf.mapper.models.event.base import Event, TaskList
from simpleflow.swf.mapper.models.event.compiler import CompiledEvent


class WorkflowType(TypedDict):
    name: str
    version: str


class WorkflowExecution(TypedDict):
    runId: str
    workflowId: str


class WorkflowExecutionEvent(Event):
    _type = "WorkflowExecution"

    initiated_event_id: int
    signal_name: str
    decision_task_completed_event_id: int


class CompiledWorkflowExecutionEvent(CompiledEvent):
    _type = "WorkflowExecution"
    states = (
        "started",  # The workflow execution was started
        "signaled",
        "completed",  # The workflow execution was closed due to successful completion
        "failed",  # The workflow execution closed due to a failure
        "timed_out",  # The workflow execution was closed because a time-out was exceeded
        "canceled",  # The workflow execution was successfully canceled and closed
        "terminated",  # The workflow execution was terminated
        "continued_as_new",  # The workflow execution was closed and a new execution of the same type was created
        # with the same workflowId
        "cancel_requested",  # A request to cancel this workflow execution was made
    )

    transitions: ClassVar[dict[str, tuple[str, ...]]] = {
        "started": (
            "signaled",
            "failed",
            "timed_out",
            "canceled",
            "terminated",
            "continued_as_new",
            "completed",
        ),
        "signaled": (
            "signaled",
            "started",
            "continued_as_new",
        ),
        "cancel_requested": ("canceled",),
    }

    initial_state = "started"


class ChildWorkflowExecutionEvent(Event):
    _type = "ChildWorkflowExecution"

    workflow_id: str
    workflow_type: WorkflowType
    initiated_event_id: int
    child_policy: str
    task_list: TaskList
    decision_task_completed_event_id: int
    # tag_list: NotRequired[list[str]]

    cause: str
    workflow_execution: WorkflowExecution

    timeout_type: str


class CompiledChildWorkflowExecutionEvent(CompiledEvent):
    _type = "ChildWorkflowExecution"

    states = (
        "start_initiated",  # A request was made to start a child workflow execution
        "start_failed",  # Failed to process start decision
        "started",  # successfully started
        "completed",  # started by this workflow execution, completed successfully and was closed
        "failed",  # started by this workflow execution, failed to complete successfully and was closed
        "timed_out",  # started by this workflow execution, timed out and was closed
        "canceled",  # started by this workflow execution, was canceled and closed
        "terminated",  # started by this workflow execution, was terminated
    )

    transitions: ClassVar[dict[str, tuple[str, ...]]] = {
        "start_initiated": ("start_failed", "started"),
        "start_failed": ("failed",),
        "started": ("canceled", "failed", "timed_out", "terminated"),
    }

    initial_state = "start_initiated"


class ExternalWorkflowExecutionEvent(Event):
    _type = "ExternalWorkflowExecution"

    initiated_event_id: int
    workflow_id: str
    signal_name: str
    cause: str
    workflow_execution: WorkflowExecution


class CompiledExternalWorkflowExecutionEvent(CompiledEvent):
    _type = "ExternalWorkflowExecution"

    states = (
        "signal_initiated",  # A request to signal an external workflow was made
        "signaled",  # A signal, requested by this workflow execution, was successfully delivered to the target
        # external workflow execution
        "signal_failed",  # The request to signal an external workflow execution failed
        "request_cancel_initiated",  # A request was made to request the cancellation of an external workflow execution
        "cancel_requested",  # Request to cancel an external workflow execution was successfully delivered to the
        # target execution
        "request_cancel_failed",  # Request to cancel an external workflow execution failed
    )

    transitions: ClassVar[dict[str, tuple[str, ...]]] = {
        "signal_initiated": ("signal_failed", "signaled"),
        "request_cancel_initiated": ("request_cancel_failed",),
        "cancel_requested": ("request_cancel_failed",),
    }

    initial_state = "signal_initiated"
