from __future__ import annotations

from typing import ClassVar

from simpleflow.swf.mapper.models.event.base import Event
from simpleflow.swf.mapper.models.event.compiler import CompiledEvent


class MarkerEvent(Event):
    _type = "Marker"

    marker_name: str
    cause: str


class CompiledMarkerEvent(CompiledEvent):
    _type = "Marker"
    states = ("recorded",)

    transitions: ClassVar[dict[str, tuple[str, ...]]] = {}
    initial_state = "recorded"
