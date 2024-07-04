from __future__ import annotations

from typing import ClassVar

from simpleflow.swf.mapper.models.event.base import Event


class InconsistentStateError(Exception):
    pass


class TransitionError(Exception):
    pass


class Stateful:
    """Base stateful object implementation"""

    states: tuple[str, ...] = ()
    transitions: ClassVar[dict[str, tuple[str, ...]]] = {}


class CompiledEvent(Event, Stateful):
    """State-aware history event base representation

    Amazon service history events comes with a type and
    a state. simpleflow.swf.mapper.models.eventEvent class already implements
    these specificities.

    But history manipulation and analysis in order to take
    decisions calls for state-aware events.

    CompiledEvent has to be instantiated against an simpleflow.swf.mapper.models.event.Event
    from which it will validate state as initial. Then, you will be able
    to apply transitions to it from events of the same type that comes next
    in the history using the .transit() method, which will
    validate the state transition is attended and valid.

    compiled events inherits from simpleflow.swf.mapper.models.compiled.event.Stateful and should
    implement

    * ``initial_state`` class attribute: constructor-supplied event attended state
    * ``states`` (tuple) class attribute: every event type possible states should
    be listed
    * ``transitions`` (dictionary) class attribute: every initial state to possible
    target state should be listed.

    Implementation **example** can be found in simpleflow.swf.mapper.models.event submodules as Compiled*Event
    classes.

    """

    initial_state: str | None = None

    def __init__(self, event):
        """Builds a  CompiledEvent from provided ``event``

        validates provided history event is in compiled event
        attended initial_state.

        :param  event: base event to build the compiled event upon
        :type   event: simpleflow.swf.mapper.models.event.Event
        """
        if event.state != self.initial_state:
            raise InconsistentStateError(
                f"Provided event is in {event.state} state when attended intial state is {self.initial_state}"
            )
        self.__dict__ = event.__dict__.copy()

    def __repr__(self):
        return f"<CompiledEvent {self.type} {self.state}>"

    @property
    def next_states(self) -> tuple[str, ...]:
        """Returns attended next compiled event states"""
        return self.transitions[self.state]

    def transit(self, event: Event):
        """Tries to apply CompiledEvent transition to the provided ``event``
        state

        :param  event: event to use in order to apply the compiled event
                       state transition
        :type   event: simpleflow.swf.mapper.models.event.Event
        """
        if event.state not in self.transitions[self.state]:
            raise TransitionError("Transition to state %s not allowed")

        self.__dict__ = event.__dict__.copy()
