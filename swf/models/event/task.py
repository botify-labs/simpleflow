# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from __future__ import annotations

import sys

if sys.version_info < (3, 8):
    from typing_extensions import TypedDict
else:
    from typing import TypedDict

from swf.models.event.base import Event
from swf.models.event.compiler import CompiledEvent


class ActivityType(TypedDict):
    name: str
    version: str


class TaskList(TypedDict):
    name: str


class ActivityTaskEvent(Event):
    _type = "ActivityTask"

    scheduled_event_id: int
    activity_id: int
    activity_type: ActivityType
    task_list: TaskList
    decision_task_completed_event_id: int
    cause: str
    identity: str
    timeout_type: str


class ActivityTaskEventDict(TypedDict):
    scheduled_event_id: int
    activity_id: int
    activity_type: ActivityType
    task_list: TaskList
    decision_task_completed_event_id: int
    cause: str


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
        "cancel_requested",  # A request_cancel decision was received by the system
        "request_cancel_failed",  # Failed to process request_cancel decision
    )

    transitions = {
        "scheduled": ("schedule_failed", "canceled", "timed_out", "started"),
        "schedule_failed": ("scheduled", "timed_out"),
        "started": ("canceled", "failed", "timed_out", "completed"),
        "failed": ("scheduled", "timed_out"),
        "timed_out": ("scheduled"),
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

    transitions = {
        "scheduled": ("started"),
        "started": ("timed_out", "completed"),
        "timed_out": ("scheduled"),
    }

    initial_state = "scheduled"
