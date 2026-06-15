from __future__ import annotations

from simpleflow import Workflow, activity, registry, task


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


class MyWorkflow(Workflow):
    name = "my_workflow"

    def run(self, *args, **kwargs):
        return args, kwargs


def test_workflow_task_stores_explicit_task_list():
    wf_task = task.WorkflowTask(None, MyWorkflow, 1, foo="bar", workflow_task_list="my_list")
    assert wf_task._task_list == "my_list"


def test_workflow_task_list_defaults_to_none():
    wf_task = task.WorkflowTask(None, MyWorkflow, 1, foo="bar")
    assert wf_task._task_list is None


def test_workflow_task_list_does_not_leak_into_run_arguments():
    # The explicit task list is a decision-routing concern; it must never be
    # forwarded as an argument to the child workflow's run().
    wf_task = task.WorkflowTask(None, MyWorkflow, 1, foo="bar", workflow_task_list="my_list")
    assert wf_task.args == [1]
    assert wf_task.kwargs == {"foo": "bar"}
    assert "workflow_task_list" not in wf_task._kwargs


def test_child_workflow_task_forwards_explicit_task_list():
    wf_task = task.ChildWorkflowTask(MyWorkflow, 1, foo="bar", workflow_task_list="my_list")
    assert wf_task._task_list == "my_list"
    assert wf_task.args == [1]
    assert wf_task.kwargs == {"foo": "bar"}
