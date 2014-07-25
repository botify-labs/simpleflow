# -*- coding: utf-8 -*-

from concurrent import futures as py_futures
from concurrent.futures._base import (
    PENDING,
    RUNNING,
    CANCELLED,
    CANCELLED_AND_NOTIFIED,
    FINISHED
)


__all__ = ['Future', 'wait']


FIRST_COMPLETED = 'FIRST_COMPLETED'
FIRST_EXCEPTION = 'FIRST_EXCEPTION'
ALL_COMPLETED = 'ALL_COMPLETED'
_AS_COMPLETED = '_AS_COMPLETED'


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


def wait(*fs):
    """Returns a list of the results of futures if there are available.
    """
    return [future.result() for future in fs]


class Future(py_futures.Future):
    """Patched version of ``concurrent.futures.Future``
    """
    def finished(self):
        with self._condition:
            if self._state == FINISHED:
                return True
            return False