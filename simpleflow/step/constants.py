"""
Default values for GetStepsDoneTask and MarkStepDoneTask.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from simpleflow.constants import HOUR, MINUTE

if TYPE_CHECKING:
    from typing import Any


STEP_ACTIVITY_PARAMS_DEFAULT: dict[str, Any] = {
    "schedule_to_start_timeout": 4 * HOUR,
    "start_to_close_timeout": 1 * MINUTE,
    "schedule_to_close_timeout": 4 * HOUR + MINUTE,
    "heartbeat_timeout": 3 * MINUTE,
    "task_priority": 100,
    "version": "1.0",
    "idempotent": True,
    "raises_on_failure": True,
    "retry": 1,
}

UNKNOWN_CONTEXT = {"run_id": "unknown", "workflow_id": "unknown", "version": "unknown"}
