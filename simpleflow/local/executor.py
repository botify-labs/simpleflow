import logging

from simpleflow import (
    exceptions,
    executor,
    futures,
)


logger = logging.getLogger(__name__)


class Executor(executor.Executor):
    """
    Executes all tasks synchronously in a single local process.

    """
    def submit(self, func, *args, **kwargs):
        logger.info('executing task {}(args={}, kwargs={})'.format(
            func, args, kwargs))
        args = [executor.get_actual_value(arg) for arg in args]
        kwargs = {key: executor.get_actual_value(val) for
                  key, val in kwargs.iteritems()}

        future = futures.Future()
        try:
            future._result = func._callable(*args, **kwargs)
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

        return self.run_workflow(*args, **kwargs)
