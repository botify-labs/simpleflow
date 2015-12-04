from __future__ import absolute_import

import json
import abc
import collections

from . import futures


def get_actual_value(value):
    """
    Unwrap the result of a Future or return the value.
    """
    if isinstance(value, futures.Future):
        return futures.get_result_or_raise(value)
    return value


class Task(object):
    """A Task represents a work that can be scheduled for execution.

    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def name(self):
        raise NotImplementedError()

    def serialize(self, value):
        return json.dumps(value)

    def resolve_args(self, *args):
        return [get_actual_value(arg) for arg in args]

    def resolve_kwargs(self, **kwargs):
        return {key: get_actual_value(val) for
                key, val in kwargs.iteritems()}


class ActivityTask(Task):
    def __init__(self, activity, *args, **kwargs):
        # TODO: check that given activity is a decorated Activity, not a raw callable
        self.activity = activity
        self.idempotent = activity.idempotent
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
        method = self.activity._callable
        if hasattr(method, 'execute'):
            return method(*self.args, **self.kwargs).execute()
        else:
            return method(*self.args, **self.kwargs)


class WorkflowTask(Task):
    def __init__(self, workflow, *args, **kwargs):
        self.workflow = workflow
        # TODO: handle idempotence at workflow level
        self.idempotent = False
        self.args = self.resolve_args(*args)
        self.kwargs = self.resolve_kwargs(**kwargs)
        self.id = None

    @property
    def name(self):
        return 'workflow-{}'.format(self.workflow.name)

    def __repr__(self):
        return '{}(workflow={}, args={}, kwargs={}, id={})'.format(
            self.__class__.__name__,
            self.activity,
            self.args,
            self.kwargs,
            self.id)


class Registry(object):
    def __init__(self):
        self._tasks = collections.defaultdict(dict)

    def __getitem__(self, label):
        return self._tasks[label]

    def register(self, task, label=None):
        self._tasks[label][task.name] = task

    def execute(self):
        raise NotImplementedError()

registry = Registry()
