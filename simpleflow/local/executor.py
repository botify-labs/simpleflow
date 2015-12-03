import logging

from simpleflow import (
    Applier,
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

        future = futures.Future()
        handler = func._callable

        _applier = Applier(handler, *args, **kwargs)
        args = _applier.args
        kwargs = _applier.kwargs

        try:
            future._result = _applier.call()
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
