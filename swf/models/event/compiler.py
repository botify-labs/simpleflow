# -*- coding:utf-8 -*-

# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from swf.models.event import Event


class InconsistentStateError(Exception):
    pass


class TransitionError(Exception):
    pass


class Stateful(object):
    """Base stateful object implementation"""
    states = ()
    transitions = {}


class CompiledEvent(Event, Stateful):
    """State-aware history event base representation

    Amazon service history events comes with a type and
    a state. swf.models.eventEvent class already implements
    these specificities.

    But history manipulation and analysis in order to take
    decisions calls for state-aware events.

    CompiledEvent has to be instantiated against an swf.models.event.Event
    from which it will validate state as initial. Then, you will be able
    to apply transitions to it from events of the same type that comes next
    in the history using the .transit() method, which will
    validate the state transition is attended and valid.

    compiled events inherits from swf.models.compiled.event.Stateful and should
    implement

    * ``initial_state`` class attribute: constructor-supplied event attended state
    * ``states`` (tuple) class attribute: every event type possible states should
    be listed
    * ``transitions`` (dictionary) class attribute: every initial state to possible
    target state should be listed.

    Implementation **example** can be found in swf.models.event submodules as Compiled*Event
    classes.

    """

    initial_state = None

    def __init__(self, event):
        """Builds a  CompiledEvent from provided ``event``

        validates provided history event is in compiled event
        attended initial_state.

        :param  event: base event to build the compiled event upon
        :type   event: swf.models.event.Event
        """
        if event.state != self.initial_state:
            raise InconsistentStateError("Provided event is in {0} state "
                                         "when attended intial state is {1}"
                                         .format(event.state, self.initial_state))
        self.__dict__ = event.__dict__.copy()

    def __repr__(self):
        return '<CompiledEvent %s %s>' % (self.type, self.state)

    @property
    def next_states(self):
        """Returns attended next compiled event states

        :rtype: list
        """
        return self.transitions[self.state]

    def transit(self, event):
        """Tries to apply CompiledEvent transition to the provided ``event``
        state

        :param  event: event to use in order to apply the compiled event
                       state transition
        :type   event: swf.models.event.Event
        """
        if event.state not in self.transitions[self.state]:
            raise TransitionError("Transition to state %s not allowed")

        self.__dict__ = event.__dict__.copy()
