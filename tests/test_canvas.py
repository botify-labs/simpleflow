from builtins import range
import unittest

from simpleflow import futures, workflow, exceptions
from simpleflow.canvas import (
    FuncGroup,
    Group,
    Chain,
    AggregateException,
)
from simpleflow.local.executor import Executor
from simpleflow.activity import with_attributes
from simpleflow.task import ActivityTask


@with_attributes()
def to_string(arg):
    return str(arg)


@with_attributes()
def to_int(arg):
    return int(arg)


@with_attributes()
def sum_values(*args):
    return sum(*args)


@with_attributes()
def sum_previous(values, previous_value):
    return sum(values) + previous_value


@with_attributes()
def running_task():
    return True


@with_attributes()
def zero_division():
    return 1 / 0


class CustomExecutor(Executor):
    """
    This executor returns a running state when
    `running_task` is called
    """

    def submit(self, func, *args, **kwargs):
        if func == running_task:
            f = futures.Future()
            f._state = futures.RUNNING
            return f
        return super(CustomExecutor, self).submit(func, *args, **kwargs)


executor = CustomExecutor(workflow.Workflow)


class TestGroup(unittest.TestCase):
    def test(self):
        future = Group(
            ActivityTask(to_string, 1),
            ActivityTask(to_string, 2)
        ).submit(executor)
        self.assertTrue(future.finished)

        future = Group(
            ActivityTask(to_string, "test1"),
            ActivityTask(running_task, "test2"),
            ActivityTask(sum_values, [1, 2])
        ).submit(executor)
        self.assertTrue(future.running)
        self.assertEquals(future.count_finished_activities, 2)
        self.assertEquals(future._result, ["test1", None, 3])
        with self.assertRaises(exceptions.ExecutionBlocked):
            dummy = future.result

    def test_exceptions(self):
        future = Group(
            ActivityTask(to_string, 1),
            ActivityTask(to_string, 2)
        ).submit(executor)
        self.assertIsNone(future.exception)

        future = Group(
            ActivityTask(zero_division),
            ActivityTask(zero_division),
        ).submit(executor)
        self.assertTrue(future.finished)
        self.assertIsInstance(future.exception, AggregateException)
        self.assertEqual(2, len(future.exception.exceptions))
        self.assertIsInstance(future.exception.exceptions[0], ZeroDivisionError)
        self.assertIsInstance(future.exception.exceptions[1], ZeroDivisionError)


class TestChain(unittest.TestCase):
    def test(self):
        future = Chain(
            ActivityTask(to_string, "test"),
            ActivityTask(to_string, "test")
        ).submit(executor)
        self.assertTrue(future.finished)
        self.assertEquals(future.count_finished_activities, 2)

        future = Chain(
            ActivityTask(to_string, "test"),
            ActivityTask(running_task, "test"),
            ActivityTask(to_string, "test")
        ).submit(executor)
        self.assertTrue(future.running)
        self.assertEquals(future.count_finished_activities, 1)

    def test_previous_value(self):
        future = Chain(
            ActivityTask(sum_values, [1, 2]),
            ActivityTask(sum_previous, [2, 3]),
            ActivityTask(sum_previous, [4, 5]),
            send_result=True
        ).submit(executor)
        self.assertTrue(future.finished)
        self.assertEquals(future.result, [3, 8, 17])

    def test_exceptions(self):
        future = Chain(
            ActivityTask(to_string, 1),
            ActivityTask(to_string, 2)
        ).submit(executor)
        self.assertIsNone(future.exception)

        future = Chain(
            ActivityTask(zero_division),
            ActivityTask(zero_division),
        ).submit(executor)
        self.assertTrue(future.finished)
        self.assertIsInstance(future.exception, AggregateException)
        # Both tasks were tried and failed (being in a chain doesn't change this)
        self.assertEqual(2, len(future.exception.exceptions))
        self.assertIsInstance(future.exception.exceptions[0], ZeroDivisionError)
        self.assertIsInstance(future.exception.exceptions[1], ZeroDivisionError)


class TestFuncGroup(unittest.TestCase):
    def test_previous_value_with_func(self):
        def custom_func(previous_value):
            group = Group()
            for i in range(0, previous_value):
                group.append(ActivityTask(to_int, i * 2))
            return group

        chain = Chain(
            ActivityTask(sum_values, [1, 2]),
            FuncGroup(custom_func),
            ActivityTask(sum_values),
            send_result=True).submit(executor)
        self.assertEquals(chain.result, [3, [0, 2, 4], 6])


class TestComplexCanvas(unittest.TestCase):
    def test(self):
        complex_canvas = Chain(
            ActivityTask(sum_values, [1, 2]),
            ActivityTask(sum_values, [1, 2]),
            Group(
                ActivityTask(to_int, 1),
                ActivityTask(to_int, 2),
            ),
            Chain(
                ActivityTask(sum_values, [1, 2]),
                ActivityTask(running_task, 1)
            ),
            ActivityTask(sum_values, [1, 2])
        )
        result = complex_canvas.submit(executor)

        self.assertFalse(result.finished)
        self.assertTrue(result.futures[0].finished)
        self.assertTrue(result.futures[1].finished)
        self.assertTrue(result.futures[2].finished)
        self.assertFalse(result.futures[3].finished)
        self.assertTrue(result.futures[3].futures[0].finished)
        self.assertFalse(result.futures[3].futures[1].finished)
        # As result.futures[3] is not finished, we shouldn't find other future
        self.assertEquals(len(result.futures), 4)

        # Change the state of the n-1 chain to make the whole
        # canvas done
        complex_canvas.activities[3].activities[1] = ActivityTask(to_int, 1)
        result = complex_canvas.submit(executor)
        self.assertTrue(result.finished)
        self.assertEquals(len(result.futures), 5)


class TestAggregateException(unittest.TestCase):
    def test_handle_all(self):
        agg_ex = AggregateException([ZeroDivisionError(), None, MemoryError()])
        agg_ex.handle(lambda ex: bool(ex))

    def test_handle_not_all(self):
        memory_error = MemoryError()
        agg_ex = AggregateException([ZeroDivisionError(), None, memory_error])

        def my_handler(ex, a, b=None):
            return type(ex) == ZeroDivisionError

        with self.assertRaises(AggregateException) as new_agg_ex:
            agg_ex.handle(my_handler, 1, b=True)
        self.assertIsInstance(new_agg_ex.exception, AggregateException)
        self.assertEqual([memory_error], new_agg_ex.exception.exceptions)

    def test_flatten(self):
        agg_ex = AggregateException(
            [
                ZeroDivisionError(), None, MemoryError(),
                AggregateException(
                    [
                        AttributeError(),
                        AggregateException(
                            [
                                ImportError(),
                            ]
                        ),
                        None,
                    ]
                ),
                AggregateException([]),
            ]
        )
        flatten_ex = agg_ex.flatten()
        self.assertEqual(
            [ZeroDivisionError, MemoryError, AttributeError, ImportError],
            [type(ex) for ex in flatten_ex.exceptions]
        )
