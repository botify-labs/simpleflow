from __future__ import annotations

import abc
import time
from copy import deepcopy
from enum import Enum
from typing import TYPE_CHECKING

import attr

from simpleflow.base import Submittable
from simpleflow.history import History
from simpleflow.utils import import_from_module

from . import futures, logger
from .activity import Activity

if TYPE_CHECKING:
    from typing import Any

    from simpleflow import Workflow
    from simpleflow.exceptions import TaskFailed
    from simpleflow.executor import Executor


def get_actual_value(value):
    """
    Unwrap the result of a Future or return the value.
    """
    if isinstance(value, futures.Future):
        return value.result
    return value


class Task(Submittable, metaclass=abc.ABCMeta):
    """A Task represents a work that can be scheduled for execution."""

    @property
    @abc.abstractmethod
    def name(self) -> str:
        raise NotImplementedError()

    @staticmethod
    def resolve_args(*args) -> list[Any]:
        return [get_actual_value(arg) for arg in args]

    @staticmethod
    def resolve_kwargs(**kwargs) -> dict[str, Any]:
        return {key: get_actual_value(val) for key, val in kwargs.items()}


class ActivityTask(Task):
    """
    Activity task.
    """

    def __init__(self, activity: Activity, *args, **kwargs):
        if not isinstance(activity, Activity):
            raise TypeError(f"Wrong value for `activity`, got {type(activity)} instead")

        self.pre_execute_funcs = []
        self.post_execute_funcs = []
        # Avoid spreading middlewares in kwargs
        self.load_middlewares(kwargs.pop("simpleflow_middlewares", None))

        # Keep original arguments for use in subclasses
        # For instance this helps casting a generic class to a simpleflow.swf.task,
        # see simpleflow.swf.task.ActivityTask.from_generic_task() factory
        self._args = deepcopy(args)
        self._kwargs = deepcopy(kwargs)

        self.activity = activity
        self.idempotent = activity.idempotent
        self.context: dict[str, Any] | None = kwargs.pop("context", None)
        self.args = self.resolve_args(*args)
        self.kwargs = self.resolve_kwargs(**kwargs)
        self.id: str | None = None

    def load_middlewares(self, middlewares):
        if not middlewares:
            return

        for pre in middlewares["pre"]:
            try:
                func = import_from_module(pre)
            except AttributeError:
                logger.exception("Cannot import a pre middleware from %r", pre)
            else:
                self.pre_execute_funcs.append(func)

        for post in middlewares["post"]:
            try:
                func = import_from_module(post)
            except AttributeError:
                logger.exception("Cannot import a post middleware from %r", post)
            else:
                self.post_execute_funcs.append(func)

    @property
    def name(self):
        return f"activity-{self.activity.name}"

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(activity={self.activity}, args={self.args}, kwargs={self.kwargs},"
            f" id={self.id})"
        )

    def execute(self):
        method = self.activity.callable

        if getattr(method, "add_context_in_kwargs", False):
            self.kwargs["context"] = self.context

        for func in self.pre_execute_funcs:
            func(self.context)

        if hasattr(method, "execute"):
            task = method(*self.args, **self.kwargs)
            task.context = self.context

            result = task.execute()

            if hasattr(task, "post_execute"):
                task.post_execute()
        else:
            # NB: the following line attaches some *state* to the callable, so it
            # can be used directly for advanced usage. This works well because we
            # don't do multithreading, but if we ever do, DANGER!
            method.context = self.context
            result = method(*self.args, **self.kwargs)

        for func in self.post_execute_funcs:
            func(self.context, result=result)

        return result

    def propagate_attribute(self, attr, val):
        """
        Propagate to the activity.
        """
        setattr(self.activity, attr, val)


class WorkflowTask(Task):
    """
    Child workflow.
    """

    def __init__(self, executor: Executor | None, workflow: type[Workflow], *args, **kwargs) -> None:
        # Keep original arguments for use in subclasses
        # For instance this helps casting a generic class to a simpleflow.swf.task,
        # see simpleflow.swf.task.WorkflowTask.from_generic_task() factory
        self._args = deepcopy(args)
        self._kwargs = deepcopy(kwargs)

        self.executor = executor
        self.workflow = workflow
        self.idempotent: bool = getattr(workflow, "idempotent", False)
        get_workflow_id = getattr(workflow, "get_workflow_id", None)
        self.args = self.resolve_args(*args)
        self.kwargs = self.resolve_kwargs(**kwargs)

        if get_workflow_id:
            self.id: str | None = get_workflow_id(workflow, *self.args, **self.kwargs)
        else:
            self.id = None

    @property
    def name(self):
        return f"workflow-{self.workflow.name}"

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(workflow={self.workflow.__module__ + '.' + self.workflow.__name__},"
            f" args={self.args}, kwargs={self.kwargs}, id={self.id})"
        )

    def execute(self) -> Any:
        workflow = self.workflow(self.executor)
        return workflow.run(*self.args, **self.kwargs)

    def propagate_attribute(self, attr: str, val: Any) -> None:
        """
        Propagate to the workflow.
        """
        setattr(self.workflow, attr, val)


class ChildWorkflowTask(WorkflowTask):
    """
    WorkflowTask subclass for cases where the executor isn't needed
    (yet).
    """

    def __init__(self, workflow: type[Workflow], *args, **kwargs) -> None:
        super().__init__(None, workflow, *args, **kwargs)


class SignalTask(Task):
    """
    Signal.
    """

    def __init__(self, name: str, *args, **kwargs) -> None:
        self._name = name
        self.args = self.resolve_args(*args)
        self.kwargs = self.resolve_kwargs(**kwargs)

    @property
    def name(self) -> str:
        return self._name

    def execute(self):
        pass


class MarkerTask(Task):
    def __init__(self, name, details: str | futures.Future | None):
        """
        :param name: Marker name
        :param details: Serializable marker details
        """
        self._name = name
        self.args = self.resolve_args(details)
        self.kwargs = {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def details(self) -> str | None:
        return self.args[0]

    def execute(self):
        pass


class TimerTask(Task):
    """
    Timer.
    """

    def __init__(self, timer_id: str, timeout: str | int, control: dict[str, Any] | None = None) -> None:
        self.timer_id = timer_id
        self.timeout = timeout
        self.control = control
        self.args: tuple = ()
        self.kwargs: dict[str, Any] = {}

    @property
    def name(self) -> str:
        return self.timer_id

    @property
    def id(self) -> str:
        return self.timer_id

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} timer_id="{self.timer_id}" timeout={self.timeout}>'

    def execute(self):
        # Local execution
        timeout = int(self.timeout) if isinstance(self.timeout, str) else self.timeout
        if timeout:
            time.sleep(timeout)


class CancelTimerTask(Task):
    """
    Timer cancellation.
    """

    def __init__(self, timer_id: str) -> None:
        self.timer_id = timer_id
        self.args: tuple = ()
        self.kwargs: dict[str, Any] = {}

    @property
    def name(self) -> str:
        return self.timer_id

    @property
    def id(self) -> str:
        return self.timer_id

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} timer_id="{self.timer_id}">'

    def execute(self) -> None:
        # Local execution: no-op
        return


@attr.s
class TaskFailureContext:
    """
    Some context for a task/workflow failure.
    """

    class Decision(Enum):
        none = 0
        abort = 1
        ignore = 2
        retry_now = 3
        retry_later = 4
        cancel = 5
        handled = 6

    a_task: ActivityTask | WorkflowTask = attr.ib()
    event: dict[str, Any] = attr.ib()
    future: futures.Future | None = attr.ib()
    exception_class: type[Exception] = attr.ib()
    history: History | None = attr.ib(default=None)
    decision: Decision | None = attr.ib(default=Decision.none)
    retry_wait_timeout: int | None = attr.ib(default=None)
    reason: str | None = attr.ib(default=None)
    details: Any = attr.ib(default=None)
    task_error: str | None = attr.ib(default=None)
    task_message: str | None = attr.ib(default=None)
    task_error_type: type[Exception] | None = attr.ib(default=None)

    def __attrs_post_init__(self):
        from simpleflow.exceptions import TaskFailed
        from simpleflow.utils import import_from_module, json_loads_or_raw

        exception = self.exception
        if isinstance(exception, TaskFailed):
            if exception.reason:
                self.reason = exception.reason
            if exception.details:
                details = json_loads_or_raw(exception.details)
                self.details = details
                if isinstance(details, dict):
                    if "error" in details:
                        self.task_error = details["error"]
                    if "message" in details:
                        self.task_message = details["message"]
                    if "error_type" in details:
                        try:
                            self.task_error_type = import_from_module(details["error_type"])
                        except Exception:  # nosec
                            pass
        else:
            self.reason = str(exception)

    @property
    def retry_count(self) -> int | None:
        return self.event.get("retry")

    @property
    def attempt_number(self) -> int:
        return self.event.get("retry", 0) + 1

    @property
    def payload(self) -> Task | None:
        return getattr(self.a_task, "payload", None)

    @property
    def task_name(self) -> str | None:
        if hasattr(self.a_task, "payload"):
            return self.a_task.payload.name
        if hasattr(self.a_task, "name"):
            return self.a_task.name
        return None

    @property
    def exception(self) -> Exception | TaskFailed | None:
        return self.future.exception if self.future.done else None

    @property
    def current_started_decision_id(self) -> int | None:
        return self.history.started_decision_id if self.history else None

    @property
    def last_completed_decision_id(self) -> int | None:
        return self.history.completed_decision_id if self.history else None

    @property
    def id(self) -> int | None:
        return History.get_event_id(self.event)

    def decide_abort(self) -> TaskFailureContext:
        self.decision = self.Decision.abort
        return self

    def decide_ignore(self) -> TaskFailureContext:
        self.decision = self.Decision.ignore
        return self

    def decide_cancel(self) -> TaskFailureContext:
        self.decision = self.Decision.cancel
        return self

    def decide_retry(self, retry_wait_timeout: int | None = 0) -> TaskFailureContext:
        self.decision = self.Decision.retry_now if not retry_wait_timeout else self.Decision.retry_later
        self.retry_wait_timeout = retry_wait_timeout
        return self

    def decide_handled(
        self, a_task: ActivityTask | WorkflowTask, future: futures.Future | None = None
    ) -> TaskFailureContext:
        self.a_task = a_task
        self.future = future
        self.decision = self.Decision.handled
        return self
