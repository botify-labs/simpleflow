# -*- coding:utf-8 -*-

# Copyright (c) 2016, Botify
#
# See the file LICENSE for copying permission.

from swf.models.event.base import Event
from swf.models.event.compiler import CompiledEvent


class LambdaFunctionEvent(Event):
    _type = 'LambdaFunction'
    _attribute_mapping = {
        'id': 'lambda_id',
        'name': 'lambda_name',
    }


class CompiledLambdaFunctionEvent(CompiledEvent):
    _type = 'LambdaFunction'
    states = (
        'scheduled',
        'schedule_failed',
        'start_failed',
        'started',
        'completed',
        'failed',
        'timed_out',
    )

    transitions = {
        'scheduled': ('schedule_failed', 'timed_out', 'start_failed', 'started'),
        'schedule_failed': ('scheduled', 'timed_out'),
        'started': ('failed', 'timed_out', 'completed'),
        'failed': ('scheduled', 'timed_out'),
        'timed_out': ('scheduled'),
    }

    initial_state = 'scheduled'
