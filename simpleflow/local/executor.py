import logging

from simpleflow import (
    Activity,
    exceptions,
    executor,
    futures,
)
from simpleflow.base import Submittable
from simpleflow.signal import WaitForSignal
from simpleflow.task import ActivityTask, WorkflowTask, SignalTask
from simpleflow.activity import Activity
from simpleflow.workflow import Workflow
from swf.models.history import builder
from simpleflow.history import History


logger = logging.getLogger(__name__)


class Executor(executor.Executor):
    """
    Executes all tasks synchronously in a single local process.

    """
    def __init__(self, workflow):
        super(Executor, self).__init__(workflow)
        self.nb_activities = 0

    @property
    def _workflow_class(self):
        """
        Returns the workflow class with all the needed attributes for
        swf.models.history.builder.History()
        This allows to get a SWF-compatible history in local executions so that
        the metrology feature works correctly.
        """
        cls = self._workflow.__class__
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

        self.initialize_history(input)

        self.before_replay()
        result = self.run_workflow(*args, **kwargs)

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
