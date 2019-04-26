import abc

from ._decorators import deprecated

if False:
    from typing import Type  # noqa
    from simpleflow import Workflow

__all__ = ['Executor']


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

    def __init__(self, workflow_class):
        # type: (Type[Workflow]) -> None
        """
        Binds the workflow's definition.

        The executor deals with the concrete execution and allows different
        backends. The workflow describes a computation. This could be also seen
        as a program, the workflow, and an interpreter, the executor.

        """
        self._workflow_class = workflow_class
        self._workflow = None

    @property
    def workflow_class(self):
        return self._workflow_class

    @property
    def workflow(self):
        return self._workflow

    def create_workflow(self):
        if self._workflow is None:
            workflow = self._workflow_class(self)
            if False:
                assert isinstance(workflow, Workflow)
            self._workflow = workflow

    def run_workflow(self, *args, **kwargs):
        """
        Runs the workflow definition.

        """
        result = self._workflow.run(*args, **kwargs)
        return result

    @abc.abstractmethod
    def submit(self, submittable, *args, **kwargs):
        """
        Submit a task for execution.

        :param task: activity or workflow.
        :type  task: base.Submittable | simpleflow.Activity | simpleflow.Workflow

        :returns:
        :rtype: :py:class:`simpleflow.futures.Future`

        """
        raise NotImplementedError

    def map(self, callable, iterable):
        """Submit *callable* with each of the items in ``*iterables``.

        All items in ``*iterable`` must be serializable in JSON.

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

    @deprecated
    def get_execution_context(self):
        return self.get_run_context()

    def get_run_context(self):
        """
        Get the run context.
        The content is specific to each executor.
        :return: context
        :rtype: dict
        """
        return {}

    @abc.abstractmethod
    def signal(self, name, *args, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def wait_signal(self, name):
        raise NotImplementedError

    @abc.abstractmethod
    def record_marker(self, name, details=None):
        raise NotImplementedError

    @abc.abstractmethod
    def list_markers(self, all=False):
        raise NotImplementedError

    @abc.abstractmethod
    def get_event_details(self, event_type, event_name):
        raise NotImplementedError
