from __future__ import annotations

from typing import ClassVar

from simpleflow.swf.mapper.models.event.base import Event
from simpleflow.swf.mapper.models.event.compiler import CompiledEvent


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

    transitions: ClassVar[dict[str, tuple[str, ...]]] = {
        "started": ("canceled", "fired"),
        "start_failed": ("canceled"),
        "fired": ("canceled"),
        "canceled": ("cancel_failed", "fired"),
    }

    initial_state = "started"
