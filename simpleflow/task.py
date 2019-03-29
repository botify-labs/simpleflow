from __future__ import absolute_import

import abc
import time
from copy import deepcopy
from enum import Enum
from typing import TYPE_CHECKING

import six

from simpleflow.base import Submittable
from simpleflow.history import History
from . import futures
from .activity import Activity

if TYPE_CHECKING:
    from typing import Optional, Any, Dict, Union, Type  # NOQA


def get_actual_value(value):
    """
    Unwrap the result of a Future or return the value.
    """
    if isinstance(value, futures.Future):
        return futures.get_result_or_raise(value)
    return value


@six.add_metaclass(abc.ABCMeta)
class Task(Submittable):
    """A Task represents a work that can be scheduled for execution.

    """
    @property
    @abc.abstractmethod
    def name(self):
        raise NotImplementedError()

    @staticmethod
    def resolve_args(*args):
        return [get_actual_value(arg) for arg in args]

    @staticmethod
    def resolve_kwargs(**kwargs):
        return {key: get_actual_value(val) for
                key, val in kwargs.items()}


class ActivityTask(Task):
    """
    Activity task.

    :type activity: Activity
    :type idempotent: Optional[bool]
    :type id: Optional[str]
    """
    def __init__(self, activity, *args, **kwargs):
        if not isinstance(activity, Activity):
            raise TypeError('Wrong value for `activity`, got {} instead'.format(type(activity)))
        # Keep original arguments for use in subclasses
        # For instance this helps casting a generic class to a simpleflow.swf.task,
        # see simpleflow.swf.task.ActivityTask.from_generic_task() factory
        self._args = deepcopy(args)
        self._kwargs = deepcopy(kwargs)

        self.activity = activity
        self.idempotent = activity.idempotent
        self.context = kwargs.pop("context", None)
        self.args = self.resolve_args(*args)
        self.kwargs = self.resolve_kwargs(**kwargs)
        self.id = None

    @property
    def name(self):
        return 'activity-{}'.format(self.activity.name)

    def __repr__(self):
        return '{}(activity={}, args={}, kwargs={}, id={})'.format(
            self.__class__.__name__,
            self.activity,
            self.args,
            self.kwargs,
            self.id)

    def execute(self):
        method = self.activity.callable

        if getattr(method, 'add_context_in_kwargs', False):
            self.kwargs["context"] = self.context

        if hasattr(method, 'execute'):
            task = method(*self.args, **self.kwargs)
            task.context = self.context
            result = task.execute()
            if hasattr(task, 'post_execute'):
                task.post_execute()
            return result
        else:
            # NB: the following line attaches some *state* to the callable, so it
            # can be used directly for advanced usage. This works well because we
            # don't do multithreading, but if we ever do, DANGER!
            method.context = self.context
            return method(*self.args, **self.kwargs)

    def propagate_attribute(self, attr, val):
        """
        Propagate to the activity.
        """
        setattr(self.activity, attr, val)


class WorkflowTask(Task):
    """
    Child workflow.

    :type executor: type(simpleflow.executor.Executor)
    :type workflow: type(simpleflow.workflow.Workflow)
    :type id: Optional[str]
    """
    def __init__(self, executor, workflow, *args, **kwargs):
        # Keep original arguments for use in subclasses
        # For instance this helps casting a generic class to a simpleflow.swf.task,
        # see simpleflow.swf.task.WorkflowTask.from_generic_task() factory
        self._args = deepcopy(args)
        self._kwargs = deepcopy(kwargs)

        self.executor = executor
        self.workflow = workflow
        self.idempotent = getattr(workflow, 'idempotent', False)
        get_workflow_id = getattr(workflow, 'get_workflow_id', None)
        self.args = self.resolve_args(*args)
        self.kwargs = self.resolve_kwargs(**kwargs)

        if get_workflow_id:
            self.id = get_workflow_id(workflow, *self.args, **self.kwargs)
        else:
            self.id = None

    @property
    def name(self):
        return 'workflow-{}'.format(self.workflow.name)

    def __repr__(self):
        return '{}(workflow={}, args={}, kwargs={}, id={})'.format(
            self.__class__.__name__,
            self.workflow.__module__ + '.' + self.workflow.__name__,
            self.args,
            self.kwargs,
            self.id)

    def execute(self):
        workflow = self.workflow(self.executor)
        return workflow.run(*self.args, **self.kwargs)

    def propagate_attribute(self, attr, val):
        """
        Propagate to the workflow.
        """
        setattr(self.workflow, attr, val)


class ChildWorkflowTask(WorkflowTask):
    """
    WorkflowTask subclass for cases where the executor isn't needed
    (yet).
    """
    def __init__(self, workflow, *args, **kwargs):
        super(ChildWorkflowTask, self).__init__(None, workflow, *args, **kwargs)


class SignalTask(Task):
    """
    Signal.
    """

    def __init__(self, name, *args, **kwargs):
        self._name = name
        self.args = self.resolve_args(*args)
        self.kwargs = self.resolve_kwargs(**kwargs)

    @property
    def name(self):
        """

        :return:
        :rtype: str
        """
        return self._name

    def execute(self):
        pass


class MarkerTask(Task):
    def __init__(self, name, details):
        """
        :param name: Marker name
        :param details: Serializable marker details
        """
        self._name = name
        self.args = self.resolve_args(details)
        self.kwargs = {}

    @property
    def name(self):
        """

        :return:
        :rtype: str
        """
        return self._name

    @property
    def details(self):
        return self.args[0]

    def execute(self):
        pass


class TimerTask(Task):
    """
    Timer.
    """

    def __init__(self, timer_id, timeout, control=None):
        self.timer_id = timer_id
        self.timeout = timeout
        self.control = control
        self.args = ()
        self.kwargs = {}

    @property
    def name(self):
        return self.timer_id

    @property
    def id(self):
        return self.timer_id

    def __repr__(self):
        return '<{} timer_id="{}" timeout={}>'.format(self.__class__.__name__, self.timer_id, self.timeout)

    def execute(self):
        # Local execution
        time.sleep(self.timeout)


class CancelTimerTask(Task):
    """
    Timer cancellation.
    """

    def __init__(self, timer_id):
        self.timer_id = timer_id
        self.args = ()
        self.kwargs = {}

    @property
    def name(self):
        return self.timer_id

    @property
    def id(self):
        return self.timer_id

    def __repr__(self):
        return '<{} timer_id="{}">'.format(self.__class__.__name__, self.timer_id)

    def execute(self):
        # Local execution: no-op
        return


class TaskFailureContext(object):
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

    def __init__(self,
                 a_task,  # type: Union[ActivityTask, WorkflowTask]
                 event,  # type: Dict[str, Any]
                 future,  # type: futures.Future
                 exception_class,  # type: Type[Exception]
                 history=None,  # type: Optional[History]
                 ):
        self.a_task = a_task
        self.event = event
        self.future = future
        self.exception_class = exception_class
        self.history = history
        self.decision = TaskFailureContext.Decision.none
        self.retry_wait_timeout = None
        self._task_error = None

    def __repr__(self):
        return '<TaskFailureContext' \
               ' task type={type}' \
               ' task.id={id}' \
               ' task.name={name}' \
               ' event={event}' \
               ' future={future}' \
               ' task_error={task_error}' \
               ' current_started_decision_id={started_decision_id}' \
               ' last_completed_decision_id={completed_decision_id}' \
               ' decision={decision}' \
               ' retry_wait_timeout={retry_wait_timeout}' \
               '>' \
            .format(
                type=type(self.a_task),
                id=getattr(self.a_task, 'id', None),
                name=getattr(self.a_task, 'name', None),
                event=self.event,
                future=self.future,
                task_error=self.task_error,
                started_decision_id=self.current_started_decision_id,
                completed_decision_id=self.last_completed_decision_id,
                decision=self.decision,
                retry_wait_timeout=self.retry_wait_timeout,
                )

    @property
    def retry_count(self):
        return self.event.get('retry')

    @property
    def task_name(self):
        if hasattr(self.a_task, 'payload'):
            return self.a_task.payload.name
        if hasattr(self.a_task, 'name'):
            return self.a_task.name
        return None

    @property
    def exception(self):
        return self.future.exception

    @property
    def current_started_decision_id(self):
        return self.history.started_decision_id if self.history else None

    @property
    def last_completed_decision_id(self):
        return self.history.completed_decision_id if self.history else None

    @property
    def task_error(self):
        if self._task_error is None:
            from simpleflow.exceptions import TaskFailed
            from simpleflow.utils import json_loads_or_raw
            self._task_error = ()  # falsy value different from None
            if isinstance(self.exception, TaskFailed) and self.exception.details:
                details = json_loads_or_raw(self.exception.details)
                if isinstance(details, dict) and 'error' in details:
                    self._task_error = details['error']
        return self._task_error

    @property
    def id(self):
        # type: () -> Optional[int]
        event = self.event
        return History.get_event_id(event)
