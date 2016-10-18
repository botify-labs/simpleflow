# -*- coding:utf-8 -*-

# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from itertools import groupby
from builtins import object, range

from future.utils import iteritems
from swf.models.event import EventFactory, CompiledEventFactory
from swf.models.event.workflow import WorkflowExecutionEvent
from swf.utils import cached_property


class History(object):
    """Execution events history container

    History object is an Event subclass objects container
    which can be built directly against an amazon json response
    using its from_event_list method.

    It is iterable and exposes a list-like __getitem__ for easier
    manipulation.

    :param  events: Events list to build History upon
    :type   events: list[swf.models.event.Event]

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

    def __init__(self, *args, **kwargs):
        self.events = kwargs.pop('events', [])
        self.raw = kwargs.pop('raw', None)
        self.it_pos = 0

    def __len__(self):
        return len(self.events)

    def __getitem__(self, val):
        if isinstance(val, int):
            return self.events[val]
        elif isinstance(val, slice):
            return History(events=self.events[val])

        raise TypeError("Unknown slice format: %s" % type(val))

    def __repr__(self):
        events_repr = '\n\t'.join(
            map(lambda e: e.__repr__(), self.events)
        )
        repr_str = '<History\n\t%s\n>' % events_repr

        return repr_str

    def __iter__(self):
        return self

    def next(self):
        """
        Iterate.
        :rtype: swf.models.event.Event
        """
        try:
            next_event = self.events[self.it_pos]
            self.it_pos += 1
        except IndexError:
            self.it_pos = 0
            raise StopIteration
        return next_event

    def __next__(self):
        """
        Py3 iterator.
        :return:
        :rtype: swf.models.event.Event
        """
        return self.next()

    @property
    def last(self):
        """Returns the last stored event

        :rtype: swf.models.event.Event
        """
        return self.events[-1]

    def latest(self, n):
        """Returns the n latest events stored in the History

        :param  n: latest events count to return
        :type   n: int

        :rtype: list
        """
        end_pos = len(self.events)
        start_pos = len(self.events) - n
        return self.events[start_pos:end_pos]

    @property
    def first(self):
        """Returns the first stored event

        :rtype: swf.models.event.Event
        """
        return self.events[0]

    @property
    def finished(self):
        """Checks if the History matches with a finished Workflow
        Execution history state.
        """
        completion_states = (
            'completed',
            'failed',
            'canceled',
            'terminated'
        )

        if isinstance(self.last, WorkflowExecutionEvent) and self.last.state in completion_states:
            return True

        return False

    def filter(self, **kwargs):
        """Filters the history based on kwargs events attributes

        Basically, allows to filter the history events upon their
        types and states. Can be used for example to retrieve every
        'DecisionTask' in the history, to check the presence of a specific
        event and so on...

        example:

        .. code-block:: python

            >>> history_obj = History()
            >>> history_obj.filter(type='ActivityTask', state='completed')  # doctest: +SKIP
            <History
                <Event 23 ActivityTask : completed>
                <Event 42 ActivityTask : completed>
                <Event 61 ActivityTask : completed>
            >
            >>> history_obj.filter(type='DecisionTask')  # doctest: +SKIP
            <History
                <Event 2 DecisionTask : scheduled>
                <Event 3 DecisionTask : started>
                <Event 7 DecisionTask : scheduled>
                <Event 8 DecisionTask : started>
                <Event 20 DecisionTask : scheduled>
                <Event 21 DecisionTask : started>
            >

        :rtype: swf.models.history.History
        """
        return filter(
            lambda e: all(getattr(e, k) == v for k, v in iteritems(kwargs)),
            self.events
        )

    @property
    def reversed(self):
        for i in range(len(self.events) - 1, -1, -1):
            yield self.events[i]

    @property
    def distinct(self):
        """Extracts distinct history events based on their types

        :rtype: list[list[swf.models.event.Event]]
        """
        distinct_events = []

        for key, group in groupby(self.events, lambda e: e.type):
            g = list(group)

            # Merge every WorkflowExecution events into same group
            if len(g) == 1 and len(distinct_events) >= 1 and g[0].type == "WorkflowExecution":
                # WorkflowExecution group will always be in first position
                distinct_events[0].extend(g)
            else:
                distinct_events.append(list(g))

        return distinct_events

    def compile(self):
        """Compiles history events into a stateful History
        based on events types and states transitions.

        Every events stored in the resulting history are stateful
        CompiledEvent subclasses instances then.

        :return: swf.models.history.History made of swf.models.event.CompiledEvent
        :rtype: swf.models.history.History
        """
        distinct_events = self.distinct
        compiled_history = []

        for events_list in distinct_events:
            if len(events_list) > 0:
                compiled_event = CompiledEventFactory(events_list[0])

                for event in events_list[1:]:
                    compiled_event.transit(event)

                compiled_history.append(compiled_event)

        return History(events=compiled_history)

    @cached_property
    def compiled(self):
        """Compiled history version

        :return: swf.models.history.History made of swf.models.event.CompiledEvent
        :rtype: swf.models.history.History
        """
        return self.compile()

    @classmethod
    def from_event_list(cls, data):
        """Instantiates a new ``swf.models.history.History`` instance
        from amazon service response.

        Every member of the History are ``swf.models.event.Event``
        subclasses instances, exposing their type, state, and so on to
        facilitate decisions according to the history.

        :param  data: event history description (typically, an amazon response)
        :type   data: dict[str, Any]

        :returns: History model instance built upon data description
        :rtype: swf.model.history.History
        """
        events_history = []

        for index, d in enumerate(data):
            event = EventFactory(d)
            events_history.append(event)

        return cls(events=events_history, raw=data)
