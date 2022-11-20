# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from __future__ import annotations

from simpleflow.utils import json_dumps
from swf.models.decision.base import Decision, decision_action


class MarkerDecision(Decision):
    _base_type = "Marker"

    @decision_action
    def record(self, name, details=None):
        """Record marker decision builder

        :param  name: name of the marker
        :type   name: str

        :param  details: Optional details of the marker.
        :type   details: Optional[dict]
        """
        if details is not None:
            details = json_dumps(details)

        self.update_attributes({"markerName": name, "details": details})
