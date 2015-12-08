from __future__ import absolute_import
from . import activity
from . import canvas
from . import task

import inspect


class Workflow(object):
    """
    Main interface to define a workflow by submitting tasks for asynchronous
    execution.

    The actual behavior depends on the executor backend.

    """
    def __init__(self, executor):
        self._executor = executor

    def submit(self, func, *args, **kwargs):
        """
        Submit a function for asynchronous execution.

        :param func: callable registered as an task.
        :type  func: activity.Activity | task.ActivityTask | canvas.Group | canvas.Chain | workflow.Workflow
        :param *args: arguments passed to the task.
        :type  *args: Sequence.
        :param **kwargs: keyword-arguments passed to the task.
        :type  **kwargs: Mapping (dict).

        :returns:
            :rtype: Future.

        """
        # If the func is a child workflow, call directly
        # the executor
        if inspect.isclass(func) and issubclass(func, Workflow):
            return self._executor.submit(func, *args, **kwargs)
        elif isinstance(func, task.ActivityTask):
            return self._executor.submit(func.activity, *func.args, **func.kwargs)
        elif isinstance(func, activity.Activity):
            return self._executor.submit(func, *args, **kwargs)
        elif isinstance(func, (canvas.Group, canvas.Chain)):
            return func.submit(self._executor)
        else:
            raise TypeError('Bad type for {} func ({})'.format(
                func,
                type(func)
            ))

    def map(self, func, iterable):
        """
        Submit a function for asynchronous execution for each value of
        *iterable*.

        :param func: callable registered as an task.
        :type  func: task.ActivityTask | task.WorkflowTask.
        :param iterable: collections of arguments passed to the task.
        :type  iterable: Iterable.

        """
        group = canvas.Group(*[task.ActivityTask(func, i) for i in iterable])
        return self.submit(group).futures

    def starmap(self, func, iterable):
        """
        Submit a function for asynchronous execution for each value of
        *iterable*.

        :param func: callable registered as an task.
        :type  func: task.ActivityTask | task.WorkflowTask.
        :param iterable: collections of multiple-arguments passed to the task
                         as positional arguments. They are destructured using
                         the ``*`` operator.
        :type  iterable: Iterable.

        """
        group = canvas.Group(*[task.ActivityTask(func, *i) for i in iterable])
        return self.submit(group).futures

    def fail(self, reason, details=None):
        self._executor.fail(reason, details)

    def before_run(self, history):
        pass

    def run(self, *args, **kwargs):
        raise NotImplementedError

    def after_run(self, history):
        pass

    def on_failure(self, history, reason, details=None):
        """
        The executor calls this method when the workflow fails.

        """
        raise NotImplementedError
