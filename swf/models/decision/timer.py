# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from __future__ import annotations

from simpleflow import format
from swf.models.decision.base import Decision, decision_action


class TimerDecision(Decision):
    _base_type = "Timer"

    @decision_action
    def start(self, id, start_to_fire_timeout, control=None):
        """Start timer decision builder

        :param  id:
        :type   id:

        :param  start_to_fire_timeout:
        :type   start_to_fire_timeout:

        :param  control: Optional data attached to the event that can
                         be used by the decider in subsequent workflow tasks
        :type   control: Optional[dict]
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
    def cancel(self, id):
        """Cancel timer decision builder

        :param  id: The unique Id of the timer to cancel
        :type   id: str
        """
        self.update_attributes({"timerId": id})
