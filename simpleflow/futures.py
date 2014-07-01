# -*- coding: utf-8 -*-

from . import exceptions


__all__ = ['Future', 'get_result_or_raise', 'wait']


FIRST_COMPLETED = 'FIRST_COMPLETED'
FIRST_EXCEPTION = 'FIRST_EXCEPTION'
ALL_COMPLETED = 'ALL_COMPLETED'
_AS_COMPLETED = '_AS_COMPLETED'

PENDING = 'PENDING'
RUNNING = 'RUNNING'
CANCELLED = 'CANCELLED'
CANCELLED_AND_NOTIFIED = 'CANCELLED_AND_NOTIFIED'
FINISHED = 'FINISHED'

_FUTURE_STATES = [
    PENDING,
    RUNNING,
    CANCELLED,
    CANCELLED_AND_NOTIFIED,
    FINISHED
]

_STATE_TO_DESCRIPTION_MAP = {
    PENDING: "pending",
    RUNNING: "running",
    CANCELLED: "cancelled",
    CANCELLED_AND_NOTIFIED: "cancelled",
    FINISHED: "finished"
}


def get_result_or_raise(future):
    """Returns the ``result`` of *future* if it is available, otherwise
    raise."""
    if future.state == PENDING:
        raise exceptions.ExecutionBlocked()
    return future.result


def wait(*fs):
    """Returns a list of the results of futures if there are available.

    Raises a ``exceptions.ExecutionBlocked`` otherwise.

    """
    if any(future.state == PENDING for future in fs):
        raise exceptions.ExecutionBlocked()

    return [future.result for future in fs]


class Future(object):
    def __init__(self):
        """Represents the state of a computation.

        It tries to mimics mod::`concurrent.futures` but involved some
        adaptations to fit the Amazon SWF model.

        """
        self._state = PENDING
        self._result = None
        self._exception = None

    def __repr__(self):
        return '<Future at %s state=%s>' % (
            hex(id(self)),
            _STATE_TO_DESCRIPTION_MAP[self._state])

    def wait(self):
        raise exceptions.ExecutionBlocked

    @property
    def result(self):
        """Raise a cls::`exceptions.ExecutionBlocked` when the result is not
        available."""
        if self._state != FINISHED:
            return self.wait()

        return self._result

    def cancel(self):
        """Cancel a future.

        Note: cannot cancel a future that is already finished.
        It will not raise an exception but return ``False`` to notify it.

        """
        if self._state == FINISHED:
            return False

        self._state = CANCELLED
        return True

    @property
    def state(self):
        return self._state

    @property
    def exception(self):
        """
        Returns `None` if no exception occurred, otherwise, returns the
        exception object that what raised by the task.

        Raise a cls::`exceptions.ExecutionBlocked` when the result is not
        available.

        """
        if self._state != FINISHED:
            return self.wait()

        return self._exception

    @property
    def cancelled(self):
        return self._state == CANCELLED

    @property
    def running(self):
        return self._state == RUNNING

    @property
    def finished(self):
        return self._state == FINISHED

    @property
    def done(self):
        return self._state in [
            CANCELLED,
            FINISHED
        ]
