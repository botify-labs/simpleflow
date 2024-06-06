from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from simpleflow.task import Task
    from simpleflow.workflow import Workflow


class ExecutionBlocked(Exception):
    pass


class TaskException(Exception):
    """
    Wrap an exception raised by a task.
    """

    def __init__(self, task: Task, exception: TaskFailed):
        """
        :param exception: raised by a task.

        """
        self.task = task
        self.exception = exception

    @property
    def payload(self):
        return self.task

    def __repr__(self):
        return f"{self.__class__.__name__}(task={self.task}, exception={self.exception})"


class WorkflowException(Exception):
    """
    Wrap an exception raised by a workflow.

    """

    def __init__(self, workflow: Workflow, exception: TaskFailed):
        """
        :param exception: raised by a workflow.
        """
        self.workflow = workflow
        self.exception = exception

    @property
    def payload(self):
        return self.workflow

    def __repr__(self):
        return f"{self.__class__.__name__}(workflow={self.workflow}, exception={self.exception})"


class TaskFailed(Exception):
    """
    Wrap the error's *reason* and *details* for a task that failed.

    :param name: of the task that failed.
    :param reason: of the failure.
    :param details: of the failure.

    """

    def __init__(self, name: str, reason: str | None, details: str | None = None):
        # NB: this is late imported else we have a circular dependency that's hard to fix
        from simpleflow.format import decode

        self.name = name
        self.reason = decode(reason, parse_json=False, use_proxy=False)
        self.details = decode(details, parse_json=False, use_proxy=False)

        super().__init__(name, self.reason, self.details)

    def __repr__(self):
        return f'{self.__class__.__name__} ({self.name}, "{self.reason}")'


class TimeoutError(Exception):
    def __init__(self, timeout_type: str = "unknown timeout", timeout_value: int | None = None):
        self.timeout_type = timeout_type
        self.timeout_value = timeout_value

    def __repr__(self):
        return f"{self.__class__.__name__}({self.timeout_type})"


class TaskCanceled(Exception):
    def __init__(self, details: str | None = None):
        self.details = details

    def __repr__(self):
        if self.details is None:  # same repr in python 2 and 3, because test :roll_eyes:
            return f"{self.__class__.__name__}()"
        return f"{self.__class__.__name__}({self.details})"


class TaskTerminated(Exception):
    pass


class AggregateException(Exception):
    """
    Class containing a list of exceptions.
    """

    def __init__(self, exceptions: list[Exception]):
        self.exceptions = exceptions

    def append(self, ex: Exception):
        self.exceptions.append(ex)

    def handle(self, handler: callable[[Exception, ...], bool], *args, **kwargs):
        """
        Invoke a user-defined handler on each exception.
        :param handler: Predicate accepting an exception and returning True if it's been handled.
        :param args: args for the handler
        :param kwargs: kwargs for the handler
        :raise: new AggregateException with the unhandled exceptions, if any
        """
        unhandled_exceptions = []
        for ex in self.exceptions:
            if ex and not handler(ex, *args, **kwargs):
                unhandled_exceptions.append(ex)
        if unhandled_exceptions:
            raise AggregateException(unhandled_exceptions)

    def flatten(self) -> AggregateException:
        """
        Flatten the AggregateException. Return a new instance without inner AggregateException.
        """
        flattened_exceptions = []
        self._flatten(self, flattened_exceptions)
        return AggregateException(flattened_exceptions)

    @staticmethod
    def _flatten(exception: Exception, exceptions: list[Exception]) -> None:
        if isinstance(exception, AggregateException):
            for ex in exception.exceptions:
                if ex:
                    AggregateException._flatten(ex, exceptions)
        else:
            exceptions.append(exception)

    def __repr__(self):
        return f"<{self.__class__.__name__} {[repr(ex) for ex in self.exceptions]!r}>"

    def __str__(self):
        return f"{self.__class__.__name__}({[str(ex) for ex in self.exceptions]!s})"

    def __eq__(self, other):
        return self.exceptions == other.exceptions


class ExecutionError(Exception):
    pass


class ExecutionTimeoutError(Exception):
    def __init__(self, command: str, timeout_value: int | float | None):
        self.timeout_command = command
        self.timeout_value = timeout_value

    def __repr__(self):
        return f"{self.__class__.__name__} after {self.timeout_value} seconds ({self.timeout_command})"

    def __str__(self):
        return self.__repr__()
