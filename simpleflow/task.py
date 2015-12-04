from __future__ import absolute_import

import json
import abc
import collections


class Task(object):
    """A Task represents a work that can be scheduled for execution.

    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def name(self):
        raise NotImplementedError()

    def serialize(self, value):
        return json.dumps(value)


class ActivityTask(Task):
    def __init__(self, activity, *args, **kwargs):
        self.activity = activity
        self.idempotent = activity.idempotent
        self.args = args
        self.kwargs = kwargs
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


class WorkflowTask(Task):
    def __init__(self, workflow, *args, **kwargs):
        self.workflow = workflow
        # TODO: handle idempotence at workflow level
        self.idempotent = False
        self.args = args
        self.kwargs = kwargs
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


registry = Registry()
