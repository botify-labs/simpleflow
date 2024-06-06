from __future__ import annotations

import sys

if sys.version_info < (3, 8):
    from typing_extensions import TypedDict
else:
    from typing import ClassVar, TypedDict

from typing import TYPE_CHECKING, Any

from simpleflow.swf.mapper.models.event.base import Event, TaskList
from simpleflow.swf.mapper.models.event.compiler import CompiledEvent

if TYPE_CHECKING:
    from datetime import datetime


class ActivityType(TypedDict):
    name: str
    version: str


class ActivityTaskEvent(Event):
    _type = "ActivityTask"

    scheduled_event_id: int
    activity_id: str
    activity_type: ActivityType
    task_list: TaskList
    decision_task_completed_event_id: int
    cause: str
    identity: str
    timeout_type: str


class ActivityTaskEventDict(TypedDict):
    type: str
    id: int
    name: str
    version: str
    state: str
    scheduled_id: int
    scheduled_timestamp: datetime
    started_id: int
    started_timestamp: datetime
    completed_id: int
    completed_timestamp: datetime
    failed_id: int
    failed_timestamp: datetime
    timeout_type: str
    timeout_value: int
    timed_out_id: int
    timed_out_timestamp: datetime
    identity: Any
    input: dict
    task_list: str
    control: dict | None
    decision_task_completed_event_id: int
    scheduled_event_id: int
    activity_id: int
    activity_type: ActivityType
    retry: int | None
    cause: str
    result: Any
    reason: str | None
    details: str | None
    cancelled_timestamp: datetime


class CompiledActivityTaskEvent(CompiledEvent):
    _type = "ActivityTask"
    states = (
        "scheduled",  # An activity task was scheduled for execution
        "schedule_failed",  # Failed to process schedule decision
        "started",  # The scheduled activity task was dispatched to a worker
        "completed",  # An activity worker successfully completed an activity task
        "failed",  # An activity worker failed an activity task
        "timed_out",  # The activity task timed out
        "canceled",  # The activity task was successfully canceled
        "cancel_requested",  # The system received a request_cancel decision
        "request_cancel_failed",  # Failed to process request_cancel decision
    )

    transitions: ClassVar[dict[str, tuple[str, ...]]] = {
        "scheduled": ("schedule_failed", "canceled", "timed_out", "started"),
        "schedule_failed": ("scheduled", "timed_out"),
        "started": ("canceled", "failed", "timed_out", "completed"),
        "failed": ("scheduled", "timed_out"),
        "timed_out": ("scheduled",),
        "canceled": ("scheduled", "timed_out"),
        "cancel_requested": ("canceled", "request_cancel_failed", "timed_out"),
        "request_cancel_failed": ("scheduled", "timed_out"),
    }

    initial_state = "scheduled"


class DecisionTaskEvent(Event):
    _type = "DecisionTask"


class CompiledDecisionTaskEvent(CompiledEvent):
    _type = "DecisionTask"
    states = (
        "scheduled",  # A decision task was scheduled for the workflow execution
        "started",  # The decision task was dispatched to a decider
        "completed",  # The decider successfully completed a decision task
        "timed_out",  # The decision task timed out
    )

    transitions: ClassVar[dict[str, tuple[str, ...]]] = {
        "scheduled": ("started",),
        "started": ("timed_out", "completed"),
        "timed_out": ("scheduled",),
    }

    initial_state = "scheduled"
