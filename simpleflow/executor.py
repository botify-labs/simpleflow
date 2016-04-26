import abc
import logging

from ._decorators import deprecated

__all__ = ['Executor']


logger = logging.getLogger(__name__)


class Executor(object):
    """
    Abstract class that describes the interface to manage the execution of
    a workflow.

    The main interface used to define a workflow is :py:meth:`Executor.submit`
    that submits a task for execution. :py:meth:`Executor.map` and
    :py:meth:`Executor.starmap` are only helpers that call
    :py:meth:`Executor.submit`.


    :py:meth:`Executor.run` performs the workflow. Please consider the
    semantics of the execution i.e.:

    - synchronous
    - asynchronous
    - asynchronous with full replay

    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, workflow):
        """
        Binds the workflow's definition.

        The executor deals with the concrete execution and allows different
        backends. The workflow describes a computation. This could be also seen
        as a program, the workflow, and an interpreter, the executor.

        """
        self._workflow = workflow(self)

    def run_workflow(self, *args, **kwargs):
        """
        Runs the workflow definition.

        """
        workflow = self._workflow
        result = workflow.run(*args, **kwargs)
        return result

    @abc.abstractmethod
    def submit(self, task, *args, **kwargs):
        """
        Submit a task for execution.

        :param task: activity or workflow.
        :type  task: :py:class:`simpleflow.Activity`
                   | :py:class:`simpleflow.Workflow`.

        :returns:
            :rtype: :py:class:`simpleflow.futures.Future`

        """
        raise NotImplementedError

    def map(self, callable, iterable):
        """Submit *callable* with each of the items in ``*iterables``.

        All items in ``*iterables`` must be serializable in JSON.

        """
        return [self.submit(callable, argument) for
                argument in iterable]

    def starmap(self, callable, iterable):
        return [self.submit(callable, *arguments) for
                arguments in iterable]

    @abc.abstractmethod
    def run(self, *args, **kwargs):
        """
        Reads (i.e.execute) the workflow's definition to execute it.

        """
        raise NotImplementedError()

    def on_failure(self, reason, details=None):
        """
        Method called when the workflow fails.

        :param reason: concise error description.
        :type  reason: str.
        :param details: optional longer error description.
        :type  details: str.

        """
        pass

    def on_completed(self):
        """
        Method called when the workflow completes.
        """
        pass

    def fail(self, reason, details=None):
        """
        Explicitly fails the workflow.

        :param reason: concise error description.
        :type  reason: str.
        :param details: optional longer error description.
        :type  details: str.

        """
        pass

    def before_replay(self):
        pass

    def after_replay(self):
        pass

    def after_closed(self):
        pass

    @deprecated
    def after_run(self):
        return self.after_closed()

    @deprecated
    def before_run(self):
        return self.before_replay()
