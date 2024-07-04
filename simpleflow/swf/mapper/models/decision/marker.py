from __future__ import annotations

from typing import Any

from simpleflow.swf.mapper.models.decision.base import Decision, decision_action
from simpleflow.utils import json_dumps


class MarkerDecision(Decision):
    _base_type = "Marker"

    @decision_action
    def record(self, name: str, details: dict[str, Any] | None = None) -> None:
        """Record marker decision builder

        :param  name: name of the marker
        :type   name: str

        :param  details: Optional details of the marker.
        :type   details: Optional[dict]
        """
        if details is not None:
            details = json_dumps(details)

        self.update_attributes({"markerName": name, "details": details})
