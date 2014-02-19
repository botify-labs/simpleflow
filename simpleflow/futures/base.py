# -*- coding: utf-8 -*-

__all__ = ['Future']


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


class Future(object):
    def __init__(self):
        """Represents the result of an asynchronous computation."""
        self._state = PENDING
        self._result = None

    def __repr__(self):
        return '<Future at %s state=%s>' % (
            hex(id(self)),
            _STATE_TO_DESCRIPTION_MAP[self._state])

    def cancel(self):
        """Cancel a future.

        Note: cannot cancel a future that is already running or finished.
        It will not raise an exception but return ``False`` to notify it.

        """
        if self._state in [RUNNING, FINISHED]:
            return False

        self._state = CANCELLED
        return True

    def cancelled(self):
        return self._state == CANCELLED

    def running(self):
        return self._state == RUNNING

    def done(self):
        return self._state in [
            CANCELLED,
            FINISHED
        ]
