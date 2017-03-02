from __future__ import absolute_import

import abc
from copy import deepcopy

from simpleflow.base import Submittable
from . import futures
from .activity import Activity


def get_actual_value(value):
    """
    Unwrap the result of a Future or return the value.
    """
    if isinstance(value, futures.Future):
        return futures.get_result_or_raise(value)
    return value


class Task(Submittable):
    """A Task represents a work that can be scheduled for execution.

    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
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
    :type id: str
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
        if hasattr(method, 'execute'):
            task = method(*self.args, **self.kwargs)
            task.context = self.context
            return task.execute()
        else:
            # NB: the following line attaches some *state* to the callable, so it
            # can be used directly for advanced usage. This works well because we
            # don't do multithreading, but if we ever do, DANGER!
            method.context = self.context
            return method(*self.args, **self.kwargs)


class WorkflowTask(Task):
    """
    Child workflow.

    :type executor: type(simpleflow.executor.Executor)
    :type workflow: type(simpleflow.workflow.Workflow)
    :type id: str
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
            if self.idempotent:
                raise Exception('"get_workflow_id" and "idempotent" are mutually exclusive')
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
