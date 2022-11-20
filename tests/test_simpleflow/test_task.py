from __future__ import annotations

from simpleflow import activity, registry, task


@activity.with_attributes(task_list="test")
def double(x):
    return x * 2


@activity.with_attributes(task_list="test")
class Double:
    def __init__(self, val):
        self.val = val

    def execute(self):
        return self.val * 2


def test_task_applies_function_correctly():
    assert task.ActivityTask(double, 2).execute() == 4


def test_task_applies_class_correctly():
    assert task.ActivityTask(Double, 4).execute() == 8


def test_context_is_empty_for_non_swf_tasks():
    assert task.ActivityTask(Double, 3).context is None


def test_task_register():
    _registry = registry.registry[None]
    assert _registry["tests.test_simpleflow.test_task.double"] == double
    assert _registry["tests.test_simpleflow.test_task.Double"] == Double
