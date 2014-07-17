import logging

import multiprocessing
from simpleflow import (
    executor,
    futures
)
from concurrent import futures as py_futures


logger = logging.getLogger(__name__)


class AdaptorFuture(futures.Future):
    """A wrapped future object that fills (some of) the semantic gap between
    `simpleflow.futures.Future` and `concurrent.futures.Future`
    """
    def __init__(self, py_future):
        super(AdaptorFuture, self).__init__()
        self.py_future = py_future

    # TODO make this method in base class call self.state()
    # def __repr__(self):
    #     return '<Future at %s state=%s>' % (
    #         hex(id(self)),
    #         _STATE_TO_DESCRIPTION_MAP[self._state])

    @property
    def result(self):
        if not self.py_future.running():
            return self.wait()

        return self._result

    def cancel(self):
        raise NotImplementedError()

    @property
    def state(self):
        if self.py_future.running():
            return futures.RUNNING
        if self.py_future.done():
            return futures.FINISHED

        return futures.PENDING

    @property
    def exception(self):
        """
        Returns `None` if no exception occurred, otherwise, returns the
        exception object that what raised by the task.

        Raise a cls::`exceptions.ExecutionBlocked` when the result is not
        available.

        """
        if not self.py_future.done():
            return self.wait()

        return self.py_future.exception()

    @property
    def cancelled(self):
        # not supported
        return False

    @property
    def running(self):
        return self.py_future.running()

    @property
    def finished(self):
        # without cancellation `finish` has the same semantic as `done`
        return self.done()

    @property
    def done(self):
        return self.py_future.done()


class Executor(executor.Executor):
    def __init__(self, workflow):
        super(Executor, self).__init__(workflow)
        # the real executor that does all the stuff
        self._executor = py_futures.ThreadPoolExecutor(
            multiprocessing.cpu_count())

    def submit(self, func, *args, **kwargs):
        logger.info('executing task {}(args={}, kwargs={})'.format(
            func, args, kwargs))
        args = [executor.get_actual_value(arg) for arg in args]
        kwargs = {key: executor.get_actual_value(val) for
                  key, val in kwargs.iteritems()}

        py_future = self._executor.submit(func, args, kwargs)

        # use the adaptor to wrap `concurrent.futures.Future`
        return AdaptorFuture(py_future)

    def run(self, input=None):
        if input is None:
            input = {}
        args = input.get('args', ())
        kwargs = input.get('kwargs', {})

        return self.run_workflow(*args, **kwargs)


if __name__ == '__main__':
    from simpleflow import activity, Workflow

    @activity.with_attributes(task_list='quickstart')
    def increment(x):
        return x + 1

    @activity.with_attributes(task_list='quickstart')
    def double(x):
        return x * 2

    class SimpleComputation(Workflow):
        def run(self, x):
            y = self.submit(increment, x)
            z = self.submit(double, y)
            return z.result

    result = Executor(SimpleComputation).run({"args": [5], "kwargs": {}})
    print result