import logging

from simpleflow import (
    exceptions,
    executor,
    futures,
)
from ..task import ActivityTask, WorkflowTask
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

        if isinstance(func, Activity):
            task = ActivityTask(func, *args, **kwargs)
        elif issubclass(func, Workflow):
            task = WorkflowTask(func, *args, __executor=self, **kwargs)
        else:
            raise Exception('Unexpected type {} for func'.format(type(func)))

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
