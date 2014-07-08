import logging

from . import (
    futures,
    exceptions,
)
from .activity import Activity
from .workflow import Workflow
from .task import (
    ActivityTask,
    WorkflowTask
)


__all__ = ['Executor', 'get_actual_value']


logger = logging.getLogger(__name__)


def get_actual_value(value):
    """Unwrap the result of a Future or return the value.

    """
    if isinstance(value, futures.Future):
        return futures.get_result_or_raise(value)
    return value


class TaskRegistry(dict):
    """This registry tracks tasks and assign them an integer identifier.

    """
    def add(self, task):
        name = task.name
        self[name] = self.setdefault(name, 0) + 1

        return self[name]


class Executor(object):
    def __init__(self, workflow):
        self._workflow = workflow(self)

        self._tasks = TaskRegistry()

    def run_workflow(self, *args, **kwargs):
        return self._workflow.run(*args, **kwargs)

    def reset(self):
        self._decisions = []
        self._tasks = TaskRegistry()

    def find_activity_event(self, task, history):
        activity = history._activities.get(task.id)
        return activity

    def find_child_workflow_event(self, task, history):
        return history._child_workflows.get(task.id)

    def make_task_id(self, task):
        """
        :returns:
            String with at most 256 characters.

        """
        index = self._tasks.add(task)
        task_id = '{name}-{idx}'.format(name=task.name, idx=index)

        return task_id

    def find_event(self, task, history):
        if isinstance(task, ActivityTask):
            return self.find_activity_event(task, history)
        elif isinstance(task, WorkflowTask):
            return self.find_child_workflow_event(task, history)
        else:
            return TypeError('invalid type {} for task {}'.format(
                type(task), task))

        return None

    def make_activity_task(self, func, *args, **kwargs):
        return ActivityTask(self, func, *args, **kwargs)

    def make_workflow_task(func, *args, **kwargs):
        return WorkflowTask(func, *args, **kwargs)

    def submit(self, func, *args, **kwargs):
        """Register a function and its arguments for asynchronous execution.

        ``*args`` and ``**kwargs`` must be serializable in JSON.

        """
        try:
            args = [get_actual_value(arg) for arg in args]
            kwargs = {key: get_actual_value(val) for
                      key, val in kwargs.iteritems()}
        except exceptions.ExecutionBlocked:
            return futures.Future()

        try:
            if isinstance(func, Activity):
                task = self.make_activity_task(func, *args, **kwargs)
            elif issubclass(func, Workflow):
                task = self.make_workflow_task(func, *args, **kwargs)
            else:
                raise TypeError
        except TypeError:
            raise TypeError('invalid type {} for {}'.format(
                type(func), func))

        return self.resume(task, *args, **kwargs)

    def map(self, callable, iterable):
        """Submit *callable* with each of the items in ``*iterables``.

        All items in ``*iterables`` must be serializable in JSON.

        """
        iterable = get_actual_value(iterable)
        return [self.submit(callable, argument) for
                argument in iterable]

    def starmap(self, callable, iterable):
        iterable = get_actual_value(iterable)
        return [self.submit(callable, *arguments) for
                arguments in iterable]

    def replay(self, history):
        raise NotImplementedError()
