from simpleflow import (
    executor,
    futures,
)


class Executor(executor.Executor):
    """
    Executes all tasks synchronously in a single local process.

    """
    def submit(self, func, *args, **kwargs):
        args = [executor.get_actual_value(arg) for arg in args]
        kwargs = {key: executor.get_actual_value(val) for
                  key, val in kwargs.iteritems()}

        future = futures.Future()
        try:
            future._result = func._callable(*args, **kwargs)
        except Exception as err:
            future._exception = err
            raise
        finally:
            future._state = futures.FINISHED

        return future

    def replay(self, input=None):
        if input is None:
            input = {}
        args = input.get('args', ())
        kwargs = input.get('kwargs', {})

        return self.run_workflow(*args, **kwargs)
