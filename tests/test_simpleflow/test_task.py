from __future__ import annotations

from simpleflow import Workflow, activity, canvas, registry, task


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


@activity.with_attributes(task_list="test", raises_on_failure=True)
def shared(x):
    return x


class SharedChildWorkflow(Workflow):
    name = "shared_child_workflow"


def test_group_attributes_land_on_the_task_not_the_activity():
    a_task = task.ActivityTask(shared, 1)
    canvas.Group(a_task, raises_on_failure=False)
    a_task.propagate_attribute("group_tag", "a")
    assert a_task.activity.raises_on_failure is False
    assert a_task.activity.group_tag == "a"
    assert a_task.activity.name == shared.name
    assert shared.raises_on_failure is True
    assert not hasattr(shared, "group_tag")
    assert a_task.activity.shared is shared


def test_tasks_of_one_activity_keep_their_own_attributes():
    optional, mandatory = task.ActivityTask(shared, 1), task.ActivityTask(shared, 2)
    optional.propagate_attribute("raises_on_failure", False)
    mandatory.propagate_attribute("raises_on_failure", True)
    assert optional.activity.raises_on_failure is False
    assert mandatory.activity.raises_on_failure is True


def test_a_later_change_to_the_activity_reaches_the_task():
    a_task = task.ActivityTask(shared, 1)
    a_task.propagate_attribute("raises_on_failure", False)
    shared.meta = {"binaries": {"tool": "v2"}}
    try:
        assert a_task.activity.meta == {"binaries": {"tool": "v2"}}
    finally:
        shared.meta = {}


def test_group_attributes_land_on_the_child_workflow_task_not_the_class():
    child = task.ChildWorkflowTask(SharedChildWorkflow)
    canvas.Group(child, raises_on_failure=False)
    assert child.workflow.raises_on_failure is False
    assert SharedChildWorkflow.raises_on_failure is True
    assert issubclass(child.workflow, SharedChildWorkflow)
    assert (child.workflow.__module__, child.workflow.__name__, child.workflow.__qualname__) == (
        SharedChildWorkflow.__module__,
        SharedChildWorkflow.__name__,
        SharedChildWorkflow.__qualname__,
    )
    assert child.name == "workflow-shared_child_workflow"
