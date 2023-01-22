from __future__ import annotations

from typing import TYPE_CHECKING

from simpleflow.base import Submittable, SubmittableContainer
from simpleflow.signal import WaitForSignal
from simpleflow.task import CancelTimerTask, TaskFailureContext, TimerTask

from . import canvas, task
from ._decorators import deprecated
from .activity import Activity
from .utils import issubclass_

if TYPE_CHECKING:
    from typing import Any

    from .marker import Marker


class Workflow(Submittable):
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
    task_priority = None
    retry = 0
    raises_on_failure = True

    INHERIT_TAG_LIST = "INHERIT_TAG_LIST"

    def __init__(self, executor):
        self._executor = executor

    @property
    def executor(self):
        return self._executor

    def submit(self, submittable, *args, **kwargs):
        """
        Submit a function for asynchronous execution.

        :param submittable: callable registered as an task.
        :type  submittable: base.Submittable
        :param args: arguments passed to the task.
        :type  args: Sequence.
        :param kwargs: keyword-arguments passed to the task.
        :type  kwargs: Mapping (dict).

        :returns:
            :rtype: simpleflow.futures.Future | simpleflow.canvas.GroupFuture

        """
        # If the activity is a child workflow, call directly
        # the executor
        if issubclass_(submittable, Workflow):
            return self._executor.submit(submittable, *args, **kwargs)
        elif isinstance(submittable, (Activity, Workflow)):
            return self._executor.submit(submittable, *args, **kwargs)
        elif isinstance(submittable, (task.Task, WaitForSignal)):
            return self._executor.submit(submittable)
        elif isinstance(submittable, SubmittableContainer):
            return submittable.submit(self._executor)
        else:
            raise TypeError(f"Bad type for {submittable} activity ({type(submittable)})")

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
        """
        Fail the workflow. User-called.
        :param reason:
        :param details:
        :return:
        """
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
        pass

    def on_completed(self, history):
        """
        Method called after successfully completing the execution.

        :param history:
        :type history: simpleflow.history.History
        """
        pass

    def on_canceled(self, history):
        """
        Method called on canceling the execution.

        :param history:
        :type history: simpleflow.history.History
        """
        pass

    def get_run_context(self):
        """
        Get a context from the executor.
        The content is specific to each executor.
        :return: context
        :rtype: dict
        """
        return self.executor.get_run_context()

    @deprecated
    def get_execution_context(self):
        """
        Get a context from the executor.
        The content is specific to each executor.
        FIXME should be get_run_context; the execution context is something else in SWF.
        :return: context
        :rtype: dict
        """
        return self.executor.get_run_context()

    def signal(self, name, *args, **kwargs):
        return self.executor.signal(name, *args, **kwargs)

    def wait_signal(self, name):
        return self.executor.wait_signal(name)

    def record_marker(self, name: str, details: Any = None) -> Submittable:
        return self.executor.record_marker(name, details)

    def list_markers(self, all: bool = False) -> list[Marker]:
        return self.executor.list_markers(all)

    def get_event_details(self, event_type: str, event_name: str) -> dict | None:
        """
        Get details about an event.
        Backend-dependent.
        The SWF backend can handle 'marker' and 'signal' events, returning a dict
        with name, input/details, event_id, ...
        :param event_type:
        :param event_name:
        :return: backend-dependent details.
        """
        return self.executor.get_event_details(event_type, event_name)

    def start_timer(self, timer_id, timeout, control=None):
        return TimerTask(timer_id, timeout, control)

    def cancel_timer(self, timer_id):
        return CancelTimerTask(timer_id)

    def should_cancel(self, history):
        """
        Called by the executor if cancel requested.
        :param history:
        :return:
        """
        return True

    def on_task_failure(
        self,
        failure_context: TaskFailureContext,
    ):
        """
        Called by the executor if a task or workflow failed.
        :param failure_context:
        :return:
        """
        pass  # no specific error handling
