import collections
import logging

from simpleflow import (
    exceptions,
    executor,
    futures,
)
from simpleflow.base import Submittable
from simpleflow.marker import Marker
from simpleflow.signal import WaitForSignal
from simpleflow.task import ActivityTask, WorkflowTask, SignalTask, MarkerTask
from simpleflow.activity import Activity
from simpleflow.workflow import Workflow
from swf.models.history import builder
from simpleflow.history import History


logger = logging.getLogger(__name__)


class Executor(executor.Executor):
    """
    Executes all tasks synchronously in a single local process.

    """

    def __init__(self, workflow_class):
        super(Executor, self).__init__(workflow_class)
        self.update_workflow_class()
        self.nb_activities = 0
        self.signals_sent = set()
        self._markers = collections.OrderedDict()

    def update_workflow_class(self):
        """
        Returns the workflow class with all the needed attributes for
        swf.models.history.builder.History()
        This allows to get a SWF-compatible history in local executions so that
        the metrology feature works correctly.
        """
        cls = self._workflow_class
        for attr in ("decision_tasks_timeout", "execution_timeout", ):
            if not hasattr(cls, attr):
                setattr(cls, attr, None)
        return cls

    def initialize_history(self, input):
        self._history = builder.History(
            self._workflow_class,
            input=input)

    def submit(self, func, *args, **kwargs):
        logger.info('executing task {}(args={}, kwargs={})'.format(
            func, args, kwargs))

        future = futures.Future()

        context = self.get_execution_context()
        context["activity_id"] = str(self.nb_activities)
        self.nb_activities += 1

        # Ensure signals ordering
        if isinstance(func, SignalTask):
            self.signals_sent.add(func.name)
        elif isinstance(func, WaitForSignal):
            signal_name = func.signal_name
            if signal_name not in self.signals_sent:
                raise NotImplementedError(
                    'wait_signal({}) before signal was sent: unsupported by the local executor'.format(signal_name)
                )
        elif isinstance(func, MarkerTask):
            self._markers.setdefault(func.name, []).append(Marker(func.name, func.details))

        if isinstance(func, Submittable):
            task = func  # *args, **kwargs already resolved.
            task.context = context
            func = getattr(task, 'activity', None)
        elif isinstance(func, Activity):
            task = ActivityTask(func, context=context, *args, **kwargs)
        elif issubclass(func, Workflow):
            task = WorkflowTask(self, func, *args, **kwargs)
        else:
            raise TypeError('invalid type {} for {}'.format(
                type(func), func))

        try:
            future._result = task.execute()
            state = 'completed'
        except Exception as err:
            future._exception = err
            logger.info('rescuing exception: {}'.format(err))
            if isinstance(func, Activity) and func.raises_on_failure:
                message = err.args[0] if err.args else ''
                raise exceptions.TaskFailed(func.name, message)
            state = 'failed'
        finally:
            future._state = futures.FINISHED

        if func:
            self._history.add_activity_task(
                func,
                decision_id=None,
                last_state=state,
                activity_id=context["activity_id"],
                input={'args': args, 'kwargs': kwargs},
                result=future._result)
        return future

    def run(self, input=None):
        if input is None:
            input = {}
        args = input.get('args', ())
        kwargs = input.get('kwargs', {})
        self.create_workflow()

        self.initialize_history(input)

        self.before_replay()
        result = self.run_workflow(*args, **kwargs)

        # Hack: self._history must be available to the callback as a
        # simpleflow.history.History, not a swf.models.history.builder.History
        self._history = History(self._history)
        self._history.parse()
        self.after_replay()
        self.on_completed()
        self.after_closed()
        return result

    def after_closed(self):
        return self._workflow.after_closed(self._history)

    def get_execution_context(self):
        return {
            "name": "local",
            "version": "1.0",
            "run_id": "local",
            "workflow_id": "local",
            "tag_list": []
        }

    def signal(self, name, *args, **kwargs):
        return SignalTask(name, *args, **kwargs)

    def wait_signal(self, name):
        return WaitForSignal(name)

    def record_marker(self, name, details=None):
        return MarkerTask(name, details)

    def list_markers(self, all=False):
        if all:
            return [m for ml in self._markers.values() for m in ml]
        return [m[-1] for m in self._markers.values()]
