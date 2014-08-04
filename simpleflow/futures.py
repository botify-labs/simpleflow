# -*- coding: utf-8 -*-

import abc
from concurrent.futures._base import (
    PENDING,
    RUNNING,
    CANCELLED,
    CANCELLED_AND_NOTIFIED,
    FINISHED
)


__all__ = ['AbstractFuture', 'wait']


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


def wait(*fs):
    """Returns a list of the results of futures if there are available.
    """
    return [future.result() for future in fs]


class AbstractFuture(object):
    """Base future class that defines an interface for concrete impls
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def result(self):
        """Return the result of the underlying computation

        The actual behavior (blocking etc.) depends on impl
        """
        raise NotImplementedError

    @abc.abstractmethod
    def exception(self):
        """Return the exception raised (if any) by the underlying computation

        The actual behavior (blocking etc.) depends on impl
        """
        raise NotImplementedError

    @abc.abstractmethod
    def running(self):
        """Return True if the underlying computation is currently executing
        """
        raise NotImplementedError

    @abc.abstractmethod
    def finished(self):
        """Return True if the underlying computation has finished
        """
        raise NotImplementedError

    @abc.abstractmethod
    def done(self):
        """Return True if the underlying compuation is cancelled or
        has finished
        """
        raise NotImplementedError