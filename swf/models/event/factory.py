# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from __future__ import annotations

import collections
from typing import TYPE_CHECKING, Any

from swf.models.event.marker import CompiledMarkerEvent, MarkerEvent
from swf.models.event.task import (
    ActivityTaskEvent,
    CompiledActivityTaskEvent,
    CompiledDecisionTaskEvent,
    DecisionTaskEvent,
)
from swf.models.event.timer import CompiledTimerEvent, TimerEvent
from swf.models.event.workflow import (
    ChildWorkflowExecutionEvent,
    CompiledChildWorkflowExecutionEvent,
    CompiledExternalWorkflowExecutionEvent,
    CompiledWorkflowExecutionEvent,
    ExternalWorkflowExecutionEvent,
    WorkflowExecutionEvent,
)
from swf.utils import camel_to_underscore, decapitalize

if TYPE_CHECKING:
    from swf.models.event import CompiledEvent, Event

EVENTS = collections.OrderedDict(
    [
        # At top-level to override 'WorkflowExecution'
        (
            "ChildWorkflowExecution",
            {
                "event": ChildWorkflowExecutionEvent,
                "compiled": CompiledChildWorkflowExecutionEvent,
            },
        ),
        (
            "ExternalWorkflow",
            {
                "event": ExternalWorkflowExecutionEvent,
                "compiled": CompiledExternalWorkflowExecutionEvent,
            },
        ),
        (
            "WorkflowExecution",
            {
                "event": WorkflowExecutionEvent,
                "compiled_event": CompiledWorkflowExecutionEvent,
            },
        ),
        (
            "DecisionTask",
            {
                "event": DecisionTaskEvent,
                "compiled_event": CompiledDecisionTaskEvent,
            },
        ),
        (
            "ActivityTask",
            {
                "event": ActivityTaskEvent,
                "compiled_event": CompiledActivityTaskEvent,
            },
        ),
        (
            "Marker",
            {
                "event": MarkerEvent,
                "compiled": CompiledMarkerEvent,
            },
        ),
        (
            "Timer",
            {
                "event": TimerEvent,
                "compiled": CompiledTimerEvent,
            },
        ),
    ]
)


class EventFactory:
    """Processes an input json event representation, and instantiates
    an ``swf.models.event.Event`` subclass instance accordingly.

    The input:

    .. code-block:: json

        {
            'eventId': 1,
            'eventType': 'DecisionTaskScheduled',
            'decisionTaskScheduledEventAttributes': {
                'startToCloseTimeout': '300',
                'taskList': {
                    'name': 'test'
                }
            },
            'eventTimestamp': 1365177769.585
        }

    will instantiate a ``swf.models.event.task.DecisionTaskEvent`` with state
    set to 'scheduled' from input attributes.

    raw_event: The input json event representation provided by
               amazon service

    :returns: ``swf.models.event.Event`` subclass instance
    """

    # eventType to Event subclass bindings
    events = EVENTS

    def __new__(cls, raw_event: dict[str, Any]) -> Event:
        event_id = raw_event["eventId"]
        event_name = raw_event["eventType"]
        event_timestamp = raw_event["eventTimestamp"]

        event_type = cls._extract_event_type(event_name)
        event_state = cls._extract_event_state(event_type, event_name)
        # amazon swf format is not very normalized and event attributes
        # response field is non-capitalized...
        event_attributes_key = decapitalize(event_name) + "EventAttributes"

        cls = EventFactory.events[event_type]["event"]
        cls._name = event_name
        cls._attributes_key = event_attributes_key

        instance = cls(
            id=event_id,
            state=event_state,
            timestamp=event_timestamp,
            raw_data=raw_event,
        )

        return instance

    @classmethod
    def _extract_event_type(cls, event_name: str) -> str | None:
        """Extracts event type from raw event_name

        :param  event_name:

        Example:

            with event_name = 'StartChildWorkflowExecutionInitiated'

        Returns:

            'ChildWorkflowExecution'

        """
        for name in cls.events:
            if name in event_name:
                return name
        return

    @classmethod
    def _extract_event_state(cls, event_type: str, event_name: str) -> str:
        """Extracts event state from raw event type and name

        Example:

            With event_name = 'StartChildWorkflowExecutionInitiated'
             and event_type = 'ChildWorkflowExecution'
            left == 'Start'
            sep == 'ChildWorkflowExecution'
            right == 'Initiated'

            Returns: 'start_initiated'

        """
        left, sep, right = event_name.partition(event_type)
        return camel_to_underscore(left + right)


class CompiledEventFactory:
    """
    Process an Event object and instantiates the corresponding
    swf.models.event.compiler.CompiledEvent.
    """

    events = EVENTS

    def __new__(cls, event) -> CompiledEvent:
        event_type = event.type

        klass = cls.events[event_type]["compiled_event"]
        instance = klass(event)

        return instance
