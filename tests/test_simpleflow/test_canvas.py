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
    """
    Special task: always running according to CustomExecutor.
    """
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
        if hasattr(func, 'activity') and func.activity == running_task:
            f = futures.Future()
            f.set_running()
            return f
        return super(CustomExecutor, self).submit(func, *args, **kwargs)


class MyWorkflow(workflow.Workflow):
    name = 'test_workflow'
    version = 'test_version'
    task_list = 'test_task_list'
    decision_tasks_timeout = '300'
    execution_timeout = '3600'


executor = CustomExecutor(MyWorkflow)
executor.initialize_history({})
executor._workflow = MyWorkflow(executor)


class TestGroup(unittest.TestCase):
    def test(self):
        future = Group(
                (to_string, 1),
                (to_string, 2)
            ).submit(executor)
        self.assertTrue(future.finished)

        future = Group(
            (to_string, "test1"),
            (running_task, "test2"),
            (sum_values, [1, 2])
        ).submit(executor)
        self.assertTrue(future.running)
        self.assertEquals(future.count_finished_activities, 2)
        self.assertEquals(future._result, ["test1", None, 3])
        with self.assertRaises(exceptions.ExecutionBlocked):
            future.result

    def test_simplified_declaration(self):
        future = Group(
            (to_string, 1),
            (to_string, 2)
        ).submit(executor)
        self.assertTrue(future.finished)

        group = Group()
        group += [
            (to_string, "test1"),
            running_task,
            (sum_values, [1, 2]),
        ]
        future = group.submit(executor)
        self.assertTrue(future.running)
        self.assertEquals(future.count_finished_activities, 2)
        self.assertEquals(future._result, ["test1", None, 3])
        with self.assertRaises(exceptions.ExecutionBlocked):
            future.result

    def test_exceptions(self):
        future = Group(
            (to_string, 1),
            (to_string, 2)
        ).submit(executor)
        self.assertIsNone(future.exception)

        future = Group(
            (zero_division),
            (zero_division),
        ).submit(executor)
        self.assertTrue(future.finished)
        self.assertIsInstance(future.exception, AggregateException)
        self.assertEqual(2, len(future.exception.exceptions))
        self.assertIsInstance(future.exception.exceptions[0], ZeroDivisionError)
        self.assertIsInstance(future.exception.exceptions[1], ZeroDivisionError)

    def test_max_parallel(self):
        future = Group(
            (running_task, "test1"),
            (running_task, "test2"),
            (running_task, "test3"),
            max_parallel=2
        ).submit(executor)
        self.assertTrue(future.running)
        self.assertEquals(len(future.futures), 2)

        future = Group(
            (to_string, "test1"),
            (running_task, "test2"),
            (running_task, "test3"),
            (running_task, "test4"),
            max_parallel=2
        ).submit(executor)
        self.assertTrue(future.running)
        self.assertEquals(len(future.futures), 3)
        self.assertEquals([f.state for f in future.futures],
                          [futures.FINISHED, futures.RUNNING, futures.RUNNING])

        future = Group(
            (to_string, "test1"),
            (to_string, "test2"),
            (to_string, "test3"),
            max_parallel=2
        ).submit(executor)
        self.assertTrue(future.finished)


class TestChain(unittest.TestCase):
    def test(self):
        future = Chain(
            (to_string, "test"),
            (to_string, "test")
        ).submit(executor)
        self.assertTrue(future.finished)
        self.assertEquals(future.count_finished_activities, 2)

        future = Chain(
            (to_string, "test"),
            (running_task, "test"),
            (to_string, "test")
        ).submit(executor)
        self.assertTrue(future.running)
        self.assertEquals(future.count_finished_activities, 1)

    def test_previous_value(self):
        future = Chain(
            (sum_values, [1, 2]),
            (sum_previous, [2, 3]),
            (sum_previous, [4, 5]),
            send_result=True
        ).submit(executor)
        self.assertTrue(future.finished)
        self.assertEquals(future.result, [3, 8, 17])

    def test_exceptions(self):
        future = Chain(
            (to_string, 1),
            (to_string, 2)
        ).submit(executor)
        self.assertIsNone(future.exception)

        # Do not execute the 3rd step is the 2nd is failing on chains
        future = Chain(
            (to_string, "test1"),
            (zero_division),
            (to_string, "test2"),
        ).submit(executor)
        self.assertTrue(future.finished)
        self.assertIsInstance(future.exception, AggregateException)
        # Both tasks were tried and failed (being in a chain doesn't change this)
        self.assertEqual(2, len(future.exception.exceptions))
        self.assertIsNone(future.exception.exceptions[0])
        self.assertIsInstance(future.exception.exceptions[1], ZeroDivisionError)

    def test_raises_on_failure(self):
        chain = Chain(
            (to_string, "test1"),
            (zero_division),
            raises_on_failure=False
        )
        self.assertFalse(chain.activities[0].activity.raises_on_failure)
        self.assertFalse(chain.activities[1].activity.raises_on_failure)

        chain = Chain(
            (to_string, "test1"),
            (zero_division),
            raises_on_failure=True
        )
        self.assertTrue(chain.activities[0].activity.raises_on_failure)
        self.assertTrue(chain.activities[1].activity.raises_on_failure)

    def test_raises_on_failure_doesnt_set_exception(self):
        future = Chain(
            (zero_division),
            (to_string, "test1"),
            raises_on_failure=False
        ).submit(executor)
        self.assertEqual(1, future.count_finished_activities)
        self.assertIsNone(future.exception)

    def test_signals_dont_hurt(self):
        """
        Check that propagate_attribute doesn't fail on signal-related objects
        :return:
        """
        future = Chain(
            (to_string, 1),
            executor.signal('test'),
            (to_string, 2),
            executor.wait_signal('test'),
            raises_on_failure=False
        ).submit(executor)
        self.assertEqual(4, future.count_finished_activities)
        self.assertIsNone(future.exception)


class TestFuncGroup(unittest.TestCase):
    def test_previous_value_with_func(self):
        def custom_func(previous_value):
            group = Group()
            for i in range(0, previous_value):
                group.append(to_int, i * 2)
            return group

        chain = Chain(
            (sum_values, [1, 2]),
            FuncGroup(custom_func),
            (sum_values,),
            send_result=True).submit(executor)
        self.assertEquals(chain.result, [3, [0, 2, 4], 6])

    def test_raises_on_failure(self):
        def custom_func():
            group = Group()
            for i in range(0, 2):
                group.append(zero_division)
            return group

        fngrp = FuncGroup(custom_func, raises_on_failure=False)
        # We have to submit the funcgroup to create
        # the activities
        fngrp.submit(executor)
        self.assertFalse(fngrp.activities.activities[0].activity.raises_on_failure)

        def custom_func():
            group = Group()
            for i in range(0, 2):
                group.append(zero_division)
            return group

        fngrp = FuncGroup(custom_func, raises_on_failure=True)
        # We have to submit the funcgroup to create
        # the activities
        with self.assertRaises(exceptions.TaskFailed):
            fngrp.submit(executor)


class TestComplexCanvas(unittest.TestCase):
    def test(self):
        complex_canvas = Chain(
            (sum_values, [1, 2]),
            (sum_values, [1, 2]),
            Group(
                (to_int, 1),
                (to_int, 2),
            ),
            Chain(
                (sum_values, [1, 2]),
                (running_task, 1)
            ),
            (sum_values, [1, 2])
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


class TestComplexCanvasSimplifiedDeclaration(unittest.TestCase):
    def test(self):
        complex_canvas = Chain(
            (sum_values, [1, 2]),
            (sum_values, [1, 2]),
            Group(
                (to_int, 1),
                (to_int, 2),
            ),
            Chain(
                (sum_values, [1, 2]),
                running_task,
            ),
            (sum_values, [1, 2])
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
