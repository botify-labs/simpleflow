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

    @abc.abstractmethod
    def schedule(self, domain):
        """
        :param domain: where the workflow is running.
        :type  domain: swf.models.Domain.

        """
        raise NotImplementedError()

    def serialize(self, value):
        return json.dumps(value)


class ActivityTask(Task):
    def __init__(self, activity, *args, **kwargs):
        self.activity = activity
        self.args = args
        self.kwargs = kwargs
        self.id = None

    @property
    def name(self):
        return 'activity-{}'.format(self.activity.name)


class WorkflowTask(Task):
    def __init__(self, workflow, *args, **kwargs):
        self.workflow = workflow
        self.args = args
        self.kwargs = kwargs
        self.id = None

    @property
    def name(self):
        return 'workflow-{}'.format(self.workflow.name)


class Registry(object):
    def __init__(self):
        self._tasks = collections.defaultdict(list)

    def __getitem__(self, label):
        return self._tasks[label]

    def register(self, task, label=None):
        self._tasks[label].append(task)


registry = Registry()
