from __future__ import annotations

from typing import Any

from simpleflow import format
from simpleflow.swf.mapper.models.decision.base import Decision, decision_action


class TimerDecision(Decision):
    _base_type = "Timer"

    @decision_action
    def start(self, id: str, start_to_fire_timeout: str, control: dict[str, Any] | None = None) -> None:
        """Start timer decision builder

        :param  id:

        :param  start_to_fire_timeout:

        :param  control: Optional data attached to the event that can
                         be used by the decider in subsequent workflow tasks
        """
        if control is not None:
            control = format.control(control)

        self.update_attributes(
            {
                "timerId": id,
                "startToFireTimeout": start_to_fire_timeout,
                "control": control,
            }
        )

    @decision_action
    def cancel(self, id: str) -> None:
        """Cancel timer decision builder

        :param  id: The unique Id of the timer to cancel
        """
        self.update_attributes({"timerId": id})
