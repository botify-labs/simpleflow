from __future__ import annotations

import unittest
from time import time
from unittest import mock

from flaky import flaky

from simpleflow.utils.retry import constant, exponential, with_delay

error_epsilon = 0.01  # tolerate an error of 0.01%
RETRY_WAIT_TIME = 0.1  # time between retries


class DummyCallable:
    def __init__(self):
        self.count = 0

    def __call__(self, *args, **kwargs):
        self.count += 1


class DummyCallableRaises(DummyCallable):
    __name__ = "DummyCallableRaises"

    def __init__(self, exception):
        super().__init__()
        self._exception = exception

    def __call__(self, *args, **kwargs):
        super().__call__(*args, **kwargs)
        raise self._exception


@flaky(max_runs=3)
class TestRetry(unittest.TestCase):
    def test_with_delay_default(self):
        callable = DummyCallableRaises(ValueError("test"))

        with self.assertRaises(ValueError):
            with_delay(delay=constant(RETRY_WAIT_TIME))(callable)()

        self.assertEqual(callable.count, 1)

    def test_with_delay(self):
        callable = DummyCallableRaises(ValueError("test"))
        max_count = 2

        t0 = time()
        with self.assertRaises(ValueError):
            with_delay(nb_times=max_count, delay=constant(RETRY_WAIT_TIME))(callable)()

        self.assertEqual(callable.count, max_count)

        total_time = time() - t0
        self.assertTrue(abs(total_time - max_count * RETRY_WAIT_TIME) <= error_epsilon * max_count)

    def test_with_delay_multiple_exceptions(self):
        callable = DummyCallableRaises(ValueError("test"))
        max_count = 3
        func = with_delay(
            nb_times=max_count,
            delay=constant(RETRY_WAIT_TIME),
            on_exceptions=[KeyError, ValueError],
        )(callable)

        t0 = time()
        with self.assertRaises(ValueError):
            func()

        self.assertEqual(callable.count, max_count)
        total_time = time() - t0
        self.assertTrue(abs(total_time - max_count * RETRY_WAIT_TIME) <= error_epsilon * max_count)

    def test_with_delay_wrong_exception(self):
        callable = DummyCallableRaises(ValueError("test"))
        with self.assertRaises(ValueError):
            with_delay(nb_times=3, delay=constant(RETRY_WAIT_TIME), on_exceptions=[KeyError])(callable)()

        # 1 == no retry
        self.assertEqual(1, callable.count)

    def test_with_delay_except(self):
        callable = DummyCallableRaises(ValueError("test"))
        with self.assertRaises(ValueError):
            with_delay(
                nb_times=3,
                delay=constant(RETRY_WAIT_TIME),
                except_on=Exception,
                on_exceptions=[ValueError],
            )(callable)()

        # 1 == no retry
        self.assertEqual(1, callable.count)

    def test_with_delay_exponential_backoff(self):
        callable = DummyCallableRaises(ValueError("test"))
        max_count = 2
        func = with_delay(nb_times=max_count, delay=exponential, on_exceptions=[KeyError, ValueError])(callable)
        with self.assertRaises(ValueError):
            with mock.patch("random.random", lambda: 0.01):
                func()

        self.assertEqual(callable.count, max_count)
