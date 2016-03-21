#! -*- coding:utf-8 -*-

# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from swf.models.event.base import Event
from swf.models.event.compiler import CompiledEvent


class MarkerEvent(Event):
    _type = 'Marker'


class CompiledMarkerEvent(CompiledEvent):
    _type = 'Marker'
    states = (
        'recorded',
    )

    transitions = {}
    initial_state = 'recorded'
