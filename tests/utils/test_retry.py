import unittest
import random
from time import time

from simpleflow.utils.retry import with_delay, constant, exponential


# TODO: fix this definition, this is not accurate (probably a number of seconds actually)
error_epsilon = 0.01  # tolerate an error of 1%


class DummyCallable(object):
    def __init__(self):
        self.count = 0

    def __call__(self, *args, **kwargs):
        self.count += 1


class DummyCallableRaises(DummyCallable):
    __name__ = 'DummyCallableRaises'

    def __init__(self, exception):
        super(DummyCallableRaises, self).__init__()
        self._exception = exception

    def __call__(self, *args, **kwargs):
        super(DummyCallableRaises, self).__call__(*args, **kwargs)
        raise self._exception


class TestRetry(unittest.TestCase):
    def test_with_delay_default(self):
        callable = DummyCallableRaises(ValueError('test'))

        with self.assertRaises(ValueError):
            with_delay()(callable)()

        self.assertEquals(callable.count, 1)

    def test_with_delay(self):
        callable = DummyCallableRaises(ValueError('test'))
        max_count = random.randint(2, 7)

        t0 = time()
        with self.assertRaises(ValueError):
            with_delay(nb_times=max_count, delay=constant(1))(callable)()

        self.assertEquals(callable.count, max_count)

        total_time = time() - t0
        self.assertTrue(abs(total_time - max_count) <= error_epsilon)

    def test_with_delay_multiple_exceptions(self):
        callable = DummyCallableRaises(ValueError('test'))
        max_count = random.randint(2, 7)
        func = with_delay(nb_times=max_count,
                          delay=constant(1),
                          on_exceptions=[KeyError, ValueError])(callable)

        t0 = time()
        with self.assertRaises(ValueError):
            func()

        self.assertEquals(callable.count, max_count)
        total_time = time() - t0
        self.assertTrue(abs(total_time - max_count) <= error_epsilon)

    def test_with_delay_exponential_backoff(self):
        callable = DummyCallableRaises(ValueError('test'))
        max_count = 3
        func = with_delay(nb_times=max_count,
                          delay=exponential,
                          on_exceptions=[KeyError, ValueError])(callable)
        with self.assertRaises(ValueError):
            func()

        self.assertEquals(callable.count, max_count)
