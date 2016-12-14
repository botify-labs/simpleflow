import logging

from simpleflow import (
    Activity,
    exceptions,
    executor,
    futures,
)
from simpleflow.base import Submittable
from simpleflow.task import ActivityTask, WorkflowTask
from simpleflow.activity import Activity
from simpleflow.workflow import Workflow


logger = logging.getLogger(__name__)


class Executor(executor.Executor):
    """
    Executes all tasks synchronously in a single local process.

    """
    def submit(self, func, *args, **kwargs):
        logger.info('executing task {}(args={}, kwargs={})'.format(
            func, args, kwargs))

        future = futures.Future()

        if isinstance(func, Submittable):
            task = func  # *args, **kwargs already resolved.
            func = task.activity  # TODO
        elif isinstance(func, Activity):
            task = ActivityTask(func, *args, **kwargs)
        elif issubclass(func, Workflow):
            task = WorkflowTask(self, func, *args, **kwargs)
        else:
            raise TypeError('invalid type {} for {}'.format(
                type(func), func))

        try:
            future._result = task.execute()
        except Exception as err:
            future._exception = err
            logger.info('rescuing exception: {}'.format(err))
            if isinstance(func, Activity) and func.raises_on_failure:
                message = err.args[0] if err.args else ''
                raise exceptions.TaskFailed(func.name, message)
        finally:
            future._state = futures.FINISHED

        return future

    def run(self, input=None):
        if input is None:
            input = {}
        args = input.get('args', ())
        kwargs = input.get('kwargs', {})

        self.before_replay()
        result = self.run_workflow(*args, **kwargs)
        self.after_replay()
        self.on_completed()
        self.after_closed()
        return result
