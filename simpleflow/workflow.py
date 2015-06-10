from __future__ import absolute_import


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
        :type  func: task.ActivityTask | task.WorkflowTask.
        :param *args: arguments passed to the task.
        :type  *args: Sequence.
        :param **kwargs: keyword-arguments passed to the task.
        :type  **kwargs: Mapping (dict).

        :returns:
            :rtype: Future.

        """
        return self._executor.submit(func, *args, **kwargs)

    def map(self, func, iterable):
        """
        Submit a function for asynchronous execution for each value of
        *iterable*.

        :param func: callable registered as an task.
        :type  func: task.ActivityTask | task.WorkflowTask.
        :param iterable: collections of arguments passed to the task.
        :type  iterable: Iterable.

        """
        return self._executor.map(func, iterable)

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
        return self._executor.starmap(func, iterable)

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
