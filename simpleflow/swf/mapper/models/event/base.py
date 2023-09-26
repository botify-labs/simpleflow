from __future__ import annotations

import sys
from datetime import datetime
from typing import Any

if sys.version_info < (3, 8):
    from typing_extensions import TypedDict
else:
    from typing import TypedDict

import pytz

from simpleflow import format
from simpleflow.swf.mapper.utils import cached_property, camel_to_underscore


class TaskList(TypedDict):
    name: str


class Event:
    """Simple workflow execution event wrapper base class

    Intends to be used as a base abstraction for the multiple amazon
    swf events implementation. Generally events implementation
    will be instantiated through the ``simpleflow.swf.mapper.models.event.factory.EventFactory``
    factory.

    It provides basic common attributes, such as the event type,
    name, json representation key to extract relevant data from,
    and sets the event id, state and timestamp from the constructor.

    Event base class is used in this project to implement
    ``simpleflow.swf.mapper.models.event.task.DecisionTaskEvent``, which a typical
    instance would for example have type 'DecisionTask',
    name 'DecisionTaskScheduleFailed', id '1' and state 'failed'.

    :param  id: event id provided by amazon service
    :param  state: event current state
    :param  timestamp: event creation timestamp
    :param  raw_data: raw_event representation provided by amazon service
    """

    _type: str | None = None
    _name: str | None = None
    _attributes_key: str | None = None
    _attributes = None

    excluded_attributes = ("eventId", "eventType", "eventTimestamp")

    def __init__(
        self,
        id: int,
        state: str,
        timestamp: float | datetime,
        raw_data: dict | None,
    ):
        """ """
        self._id = id
        self._state = state
        self._timestamp = timestamp
        self._input: dict = {}
        self._control: dict | None = None
        self.raw = raw_data or {}

        self.process_attributes()

    def __repr__(self):
        return f"<Event {self.id} {self.type} : {self.state} >"

    @property
    def id(self) -> int:
        return self._id

    @property
    def type(self) -> str:
        return self._type

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> str:
        return self._state

    @cached_property
    def timestamp(self) -> datetime:
        if isinstance(self._timestamp, datetime):
            return self._timestamp.astimezone(pytz.UTC)
        return datetime.fromtimestamp(self._timestamp, tz=pytz.UTC)

    @property
    def input(self) -> dict[str, Any]:
        return self._input

    @input.setter
    def input(self, value):
        self._input = format.decode(value)

    @property
    def control(self) -> dict[str, Any] | None:
        return self._control

    @control.setter
    def control(self, value):
        self._control = format.decode(value)

    def process_attributes(self):
        """Processes the event raw_data attributes_key elements
        and sets current instance attributes accordingly"""
        for key, value in self.raw[self._attributes_key].items():
            setattr(self, camel_to_underscore(key), value)
