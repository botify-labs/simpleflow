from __future__ import annotations

from itertools import groupby
from typing import Any, Iterator

from simpleflow.swf.mapper.models.event.base import Event
from simpleflow.swf.mapper.models.event.compiler import CompiledEvent
from simpleflow.swf.mapper.models.event.factory import (
    CompiledEventFactory,
    EventFactory,
)
from simpleflow.swf.mapper.models.event.workflow import WorkflowExecutionEvent
from simpleflow.swf.mapper.utils import cached_property


class History:
    """Execution events history container

    History object is an Event subclass objects container
    which can be built directly against an amazon json response
    using its from_event_list method.

    It is iterable and exposes a list-like __getitem__ for easier
    manipulation.

    :param  events: Events list to build History upon
    :type   events: list[simpleflow.swf.mapper.models.event.Event]

    Typical amazon response looks like:

    .. code-block:: json

        {
            "events": [
                {
                    'eventId': 1,
                    'eventType': 'WorkflowExecutionStarted',
                    'workflowExecutionStartedEventAttributes': {
                        'taskList': {
                            'name': 'test'
                        },
                        'parentInitiatedEventId': 0,
                        'taskStartToCloseTimeout': '300',
                        'childPolicy': 'TERMINATE',
                        'executionStartToCloseTimeout': '6000',
                        'workflowType': {
                            'version': '0.1',
                            'name': 'test-1'
                        },
                    },
                    'eventTimestamp': 1365177769.585,
                },
                {
                    'eventId': 2,
                    'eventType': 'DecisionTaskScheduled',
                    'decisionTaskScheduledEventAttributes': {
                        'startToCloseTimeout': '300',
                        'taskList': {
                            'name': 'test'
                        }
                    },
                    'eventTimestamp': 1365177769.585
                }
            ]
        }
    """

    def __init__(self, *args, **kwargs) -> None:
        self.events = kwargs.pop("events", [])
        self.raw = kwargs.pop("raw", None)
        self.it_pos = 0

    def __len__(self) -> int:
        return len(self.events)

    def __getitem__(self, val: int | slice) -> Event | History:
        if isinstance(val, int):
            return self.events[val]
        elif isinstance(val, slice):
            return History(events=self.events[val])

        raise TypeError(f"Unknown slice format: {type(val)}")

    def __repr__(self):
        events_repr = "\n\t".join([e.__repr__() for e in self.events])
        repr_str = f"<History\n\t{events_repr}\n>"

        return repr_str

    def __iter__(self):
        return self

    def __next__(self) -> Event:
        """
        Iterate.
        """
        try:
            next_event = self.events[self.it_pos]
            self.it_pos += 1
        except IndexError:
            self.it_pos = 0
            raise StopIteration
        return next_event

    @property
    def last(self) -> Event:
        """Returns the last stored event

        :rtype: simpleflow.swf.mapper.models.event.Event
        """
        return self.events[-1]

    def latest(self, n: int) -> list[Event]:
        """Returns the n latest events stored in the History

        :param  n: latest events count to return
        """
        end_pos = len(self.events)
        start_pos = len(self.events) - n
        return self.events[start_pos:end_pos]

    @property
    def first(self) -> Event:
        """Returns the first stored event"""
        return self.events[0]

    @property
    def finished(self) -> bool:
        """Checks if the History matches with a finished Workflow
        Execution history state.
        """
        completion_states = {"completed", "failed", "canceled", "terminated"}

        if isinstance(self.last, WorkflowExecutionEvent) and self.last.state in completion_states:
            return True

        return False

    def filter(self, **kwargs) -> list[Event]:
        """Filters the history based on kwargs events attributes

        Basically, allows to filter the history events upon their
        types and states. Can be used for example to retrieve every
        'DecisionTask' in the history, to check the presence of a specific
        event and so on...

        example:

        .. code-block:: python

            >>> history_obj = History()
            >>> history_obj.filter(type='ActivityTask', state='completed')  # doctest: +SKIP
            [
                <Event 23 ActivityTask : completed>,
                <Event 42 ActivityTask : completed>,
                <Event 61 ActivityTask : completed>,
            ]
            >>> history_obj.filter(type='DecisionTask')  # doctest: +SKIP
            [
                <Event 2 DecisionTask : scheduled>,
                <Event 3 DecisionTask : started>,
                <Event 7 DecisionTask : scheduled>,
                <Event 8 DecisionTask : started>,
                <Event 20 DecisionTask : scheduled>,
                <Event 21 DecisionTask : started>,
            ]
        """
        return [e for e in self.events if all(getattr(e, k) == v for k, v in kwargs.items())]

    @property
    def reversed(self) -> Iterator[Event]:
        for i in range(len(self.events) - 1, -1, -1):
            yield self.events[i]

    @property
    def distinct(self) -> list[list[Event]]:
        """Extracts distinct history events based on their types"""
        distinct_events: list[list[Event]] = []

        for key, group in groupby(self.events, lambda e: e.type):
            g = list(group)

            # Merge every WorkflowExecution events into same group
            if len(g) == 1 and len(distinct_events) >= 1 and g[0].type == "WorkflowExecution":
                # WorkflowExecution group will always be in first position
                distinct_events[0].extend(g)
            else:
                distinct_events.append(list(g))

        return distinct_events

    def compile(self) -> History:
        """Compiles history events into a stateful History
        based on events types and states transitions.

        Every event stored in the resulting history are stateful
        CompiledEvent subclasses instances then.

        :return: simpleflow.swf.mapper.models.history.History made of simpleflow.swf.mapper.models.event.CompiledEvent
        """
        distinct_events = self.distinct
        compiled_history: list[CompiledEvent] = []

        for events_list in distinct_events:
            if len(events_list) > 0:
                compiled_event = CompiledEventFactory(events_list[0])

                for event in events_list[1:]:
                    compiled_event.transit(event)

                compiled_history.append(compiled_event)

        return History(events=compiled_history)

    @cached_property
    def compiled(self) -> History:
        """Compiled history version

        :return: simpleflow.swf.mapper.models.history.History made of simpleflow.swf.mapper.models.event.CompiledEvent
        """
        return self.compile()

    @classmethod
    def from_event_list(cls, data: list[dict[str, Any]]) -> History:
        """Instantiates a new ``simpleflow.swf.mapper.models.history.History`` instance
        from amazon service response.

        Every member of the History are ``simpleflow.swf.mapper.models.event.Event``
        subclasses instances, exposing their type, state, and so on to
        facilitate decisions according to the history.

        :param  data: event history description (typically, an amazon response)

        :returns: History model instance built upon data description
        """
        events_history: list[Event] = []

        for index, d in enumerate(data):
            event = EventFactory(d)
            events_history.append(event)

        return cls(events=events_history, raw=data)
