# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from __future__ import annotations

from datetime import datetime

import pytz

from simpleflow import format
from swf.utils import cached_property, camel_to_underscore


class Event:
    """Simple workflow execution event wrapper base class

    Intends to be used as a base abstraction for the multiple amazon
    swf events implementation. Generally events implementation
    will be instantiated through the ``swf.models.event.factory.EventFactory``
    factory.

    It provides basic common attributes, such as the event type,
    name, json representation key to extract relevant data from,
    and sets the event id, state and timestamp from the constructor.

    Event base class is used in this project to implement
    ``swf.models.event.task.DecisionTaskEvent``, which a typical
    instance would for example have type 'DecisionTask',
    name 'DecisionTaskScheduleFailed', id '1' and state 'failed'.

    :param  id: event id provided by amazon service
    :type   id: string

    :param  state: event current state
    :type   state: string

    :param  timestamp: event creation timestamp
    :type   timestamp: float

    :param  raw_data: raw_event representation provided by amazon service
    :type   raw_data: dict
    """

    _type = None
    _name = None
    _attributes_key = None
    _attributes = None

    excluded_attributes = ("eventId", "eventType", "eventTimestamp")

    def __init__(self, id, state, timestamp, raw_data):
        """ """
        self._id = id
        self._state = state
        self._timestamp = timestamp
        self._input = {}
        self._control = None
        self.raw = raw_data or {}

        self.process_attributes()

    def __repr__(self):
        return f"<Event {self.id} {self.type} : {self.state} >"

    @property
    def id(self):
        return self._id

    @property
    def type(self):
        return self._type

    @property
    def name(self):
        return self._name

    @property
    def state(self):
        return self._state

    @cached_property
    def timestamp(self):
        return datetime.fromtimestamp(self._timestamp, tz=pytz.UTC)

    @property
    def input(self):
        return self._input

    @input.setter
    def input(self, value):
        self._input = format.decode(value)

    @property
    def control(self):
        return self._control

    @control.setter
    def control(self, value):
        self._control = format.decode(value)

    def process_attributes(self):
        """Processes the event raw_data attributes_key elements
        and sets current instance attributes accordingly"""
        for key, value in self.raw[self._attributes_key].items():
            setattr(self, camel_to_underscore(key), value)
