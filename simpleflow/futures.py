from __future__ import annotations

from simpleflow import exceptions
from simpleflow._decorators import deprecated

__all__ = ["Future", "get_result_or_raise", "wait"]


FIRST_COMPLETED = "FIRST_COMPLETED"
FIRST_EXCEPTION = "FIRST_EXCEPTION"
ALL_COMPLETED = "ALL_COMPLETED"
_AS_COMPLETED = "_AS_COMPLETED"

PENDING = "PENDING"
RUNNING = "RUNNING"
CANCELLED = "CANCELLED"
CANCELLED_AND_NOTIFIED = "CANCELLED_AND_NOTIFIED"
FINISHED = "FINISHED"

_FUTURE_STATES = [PENDING, RUNNING, CANCELLED, CANCELLED_AND_NOTIFIED, FINISHED]

_STATE_TO_DESCRIPTION_MAP = {
    PENDING: "pending",
    RUNNING: "running",
    CANCELLED: "cancelled",
    CANCELLED_AND_NOTIFIED: "cancelled",
    FINISHED: "finished",
}


@deprecated
def get_result_or_raise(future):
    """Returns the ``result`` of *future* if it is available, otherwise
    raise.
    """
    return future.result


def wait(*fs):
    """Returns a list of the results of futures if there are available.

    Raises a ``exceptions.ExecutionBlocked`` otherwise.

    """
    if any(future.state == PENDING for future in fs):
        raise exceptions.ExecutionBlocked()

    return [future.result for future in fs]


class Future:
    def __init__(self):
        """Represents the state of a computation.

        It tries to mimics mod::`concurrent.futures` but involved some
        adaptations to fit the Amazon SWF model.

        """
        self._state = PENDING
        self._result = None
        self._exception = None

    def __repr__(self):
        return "<Future at {} state={}{}>".format(
            hex(id(self)),
            _STATE_TO_DESCRIPTION_MAP[self._state],
            " exception=%r" % self._exception if self._exception else "",
        )

    def wait(self):
        raise exceptions.ExecutionBlocked

    @property
    def result(self):
        """Raise a cls::`exceptions.ExecutionBlocked` when the result is not
        available and the future was not cancelled."""
        if self.done:
            return self._result
        return self.wait()

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
    def pending(self):
        return self._state == PENDING

    @property
    def done(self):
        return self._state in [CANCELLED, FINISHED]

    # Internal methods
    def set_running(self):
        self._state = RUNNING

    def set_exception(self, exception):
        """
        Set state to finished with an exception.
        :param exception:
        :type exception: Optional[Exception]
        :return:
        """
        self._state = FINISHED
        self._exception = exception

    def set_finished(self, result):
        self._state = FINISHED
        self._result = result

    def set_cancelled(self):
        self._state = CANCELLED
