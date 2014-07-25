from simpleflow import exceptions
from simpleflow.futures import (
    FINISHED,
    CANCELLED,
    PENDING,
    RUNNING,
    Future as BaseFuture
)


class Future(BaseFuture):
    """Future with special Simple Workflow semantics
    """
    @classmethod
    def wait(cls):
        raise exceptions.ExecutionBlocked

    def result(self, timeout=None):
        with self._condition:
            if self._state != FINISHED:
                return self.wait()
            # TODO what happen if cancelled ???
            return self._result

    def exception(self, timeout=None):
        with self._condition:
            if self._state != FINISHED:
                return self.wait()
            return self._exception

    def cancel(self):
        with self._condition:
            if self._state == FINISHED:
                return False
            self._state = CANCELLED
            return True