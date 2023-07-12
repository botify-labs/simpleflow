# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from __future__ import annotations

from swf.models.event.base import Event
from swf.models.event.compiler import CompiledEvent


class TimerEvent(Event):
    _type = "Timer"

    timer_id: str
    start_to_fire_timeout: str
    cause: str
    decision_task_completed_event_id: int


class CompiledTimerEvent(CompiledEvent):
    _type = "Timer"

    states = (
        "started",  # A timer was started for the workflow execution
        "start_failed",  # Failed to process StartTimer decision
        "fired",  # A timer, previously started for this workflow execution, fired
        "canceled",  # A timer, previously started for this workflow execution, was successfully canceled
        "cancel_failed",  # Failed to process CancelTimer decision
    )

    transitions = {
        "started": ("canceled", "fired"),
        "start_failed": ("canceled"),
        "fired": ("canceled"),
        "canceled": ("cancel_failed", "fired"),
    }

    initial_state = "started"
