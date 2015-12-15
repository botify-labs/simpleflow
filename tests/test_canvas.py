import unittest

from simpleflow import futures, workflow, exceptions
from simpleflow.canvas import (
    FuncGroup,
    Group,
    Chain
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
            future.result


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


class TestFuncGroup(unittest.TestCase):
    def test_previous_value_with_func(self):
        def custom_func(previous_value):
            group = Group()
            for i in xrange(0, previous_value):
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
