import logging

from simpleflow import (
    exceptions,
    executor,
    futures,
)
from ..task import ActivityTask


logger = logging.getLogger(__name__)


class Executor(executor.Executor):
    """
    Executes all tasks synchronously in a single local process.

    """
    def submit(self, func, *args, **kwargs):
        logger.info('executing task {}(args={}, kwargs={})'.format(
            func, args, kwargs))

        future = futures.Future()

        task = ActivityTask(func, *args, **kwargs)

        try:
            future._result = task.execute()
        except Exception as err:
            future._exception = err
            if func.raises_on_failure:
                raise exceptions.TaskFailed(func.name, err.message)
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
