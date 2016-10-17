from __future__ import absolute_import

from . import canvas
from . import task
from ._decorators import deprecated
from .activity import Activity
from .utils import issubclass_


class Workflow(object):
    """
    Main interface to define a workflow by submitting tasks for asynchronous
    execution.

    The actual behavior depends on the executor backend.

    :type executor: simpleflow.executor.Executor

    """

    # These are needed for workflow on SWF
    name = None
    version = None
    task_list = None
    tag_list = None
    child_policy = None
    execution_timeout = None

    def __init__(self, executor):
        self._executor = executor

    @property
    def executor(self):
        return self._executor

    def submit(self, activity, *args, **kwargs):
        """
        Submit a function for asynchronous execution.

        :param activity: callable registered as an task.
        :type  activity: Activity | task.ActivityTask | task.WorkflowTask | canvas.Group | canvas.Chain | Workflow
        :param args: arguments passed to the task.
        :type  args: Sequence.
        :param kwargs: keyword-arguments passed to the task.
        :type  kwargs: Mapping (dict).

        :returns:
            :rtype: simpleflow.futures.Future | simpleflow.canvas.GroupFuture

        """
        # If the activity is a child workflow, call directly
        # the executor
        if issubclass_(activity, Workflow):
            return self._executor.submit(activity, *args, **kwargs)
        elif isinstance(activity, (task.ActivityTask, task.WorkflowTask)):
            return self._executor.submit(activity.activity, *activity.args, **activity.kwargs)
        elif isinstance(activity, Activity):
            return self._executor.submit(activity, *args, **kwargs)
        elif isinstance(activity, canvas.Group):
            return activity.submit(self._executor)
        else:
            raise TypeError('Bad type for {} activity ({})'.format(
                activity,
                type(activity)
            ))

    def map(self, activity, iterable):
        """
        Submit an activity for asynchronous execution for each value of
        *iterable*.

        :param activity: activity.
        :type  activity: Activity
        :param iterable: collections of arguments passed to the task.
        :type  iterable: collection.Iterable[Any]
        :rtype: list[simpleflow.futures.Future]

        """
        group = canvas.Group(*[task.ActivityTask(activity, i) for i in iterable])
        return self.submit(group).futures

    def starmap(self, activity, iterable):
        """
        Submit an activity for asynchronous execution for each value of
        *iterable*.

        :param activity: activity.
        :type  activity: Activity
        :param iterable: collections of multiple-arguments passed to the task
                         as positional arguments. They are destructured using
                         the ``*`` operator.
        :type  iterable: collection.Iterable[Any]
        :rtype: list[simpleflow.futures.Future]

        """
        group = canvas.Group(*[task.ActivityTask(activity, *i) for i in iterable])
        return self.submit(group).futures

    def fail(self, reason, details=None):
        self._executor.fail(reason, details)

    def before_replay(self, history):
        """
        Method called before playing the execution.

        :param history:
        :type history: simpleflow.history.History
        """
        pass

    def after_replay(self, history):
        """
        Method called after playing the execution.
        Either the replay is finished or the execution is blocked.

        :param history:
        :type history: simpleflow.history.History
        """
        pass

    def after_closed(self, history):
        """
        Method called after closing the execution.
        Either the replay is finished or it failed.

        :param history:
        :type history: simpleflow.history.History
        """
        pass

    @deprecated
    def before_run(self, history):
        return self.before_replay(history)

    @deprecated
    def after_run(self, history):
        return self.after_closed(history)

    def run(self, *args, **kwargs):
        raise NotImplementedError

    def on_failure(self, history, reason, details=None):
        """
        Method called after the workflow failed.

        :param history:
        :type history: simpleflow.history.History
        :param reason: failure reason
        :type reason: str
        :param details:
        :type details: Optional[str]
        """
        raise NotImplementedError

    def on_completed(self, history):
        """
        Method called after successfully completing the execution.

        :param history:
        :type history: simpleflow.history.History
        """
        raise NotImplementedError
