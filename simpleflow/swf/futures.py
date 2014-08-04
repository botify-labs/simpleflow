from simpleflow import exceptions
from simpleflow.futures import (
    FINISHED,
    CANCELLED,
    PENDING,
    RUNNING,
    CANCELLED_AND_NOTIFIED,
    AbstractFuture
)


_STATE_TO_DESCRIPTION_MAP = {
    PENDING: "pending",
    RUNNING: "running",
    CANCELLED: "cancelled",
    CANCELLED_AND_NOTIFIED: "cancelled",
    FINISHED: "finished"
}


class Future(AbstractFuture):
    """Future impl that contains Simple Workflow specific logic
    """

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

    @classmethod
    def wait(cls):
        raise exceptions.ExecutionBlocked

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

    def state(self):
        return self._state

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

    def cancelled(self):
        return self._state == CANCELLED

    def running(self):
        return self._state == RUNNING

    def finished(self):
        return self._state == FINISHED

    def done(self):
        return self._state in [
            CANCELLED,
            FINISHED
        ]