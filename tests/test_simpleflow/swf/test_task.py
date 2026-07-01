from __future__ import annotations

from simpleflow import Workflow, activity, task
from simpleflow.swf.task import ActivityTask, WorkflowTask


@activity.with_attributes()
def show_context_func():
    return show_context_func.context


@activity.with_attributes()
class ShowContextCls:
    def execute(self):
        return self.context


def test_task_has_an_empty_context_by_default():
    assert ActivityTask(show_context_func).execute() is None
    assert ActivityTask(ShowContextCls).execute() is None


def test_task_attaches_context_to_functions():
    ctx = {"foo": "bar"}
    assert ActivityTask(show_context_func, context=ctx).execute() == ctx
    assert show_context_func.context == ctx


def test_task_attaches_context_to_object_instances():
    ctx = {"foo": "bar"}
    assert ActivityTask(ShowContextCls, context=ctx).execute() == ctx
    assert ShowContextCls.context is None


class PlainWorkflow(Workflow):
    name = "plain_workflow"

    def run(self, *args, **kwargs):
        pass


class TaskListWorkflow(Workflow):
    name = "task_list_workflow"
    task_list = "from_attribute"

    def run(self, *args, **kwargs):
        pass


class GetTaskListWorkflow(Workflow):
    name = "get_task_list_workflow"

    @staticmethod
    def get_task_list(workflow, *args, **kwargs):
        return "from_get_task_list"

    def run(self, *args, **kwargs):
        pass


def test_task_list_defaults_to_none():
    assert WorkflowTask(None, PlainWorkflow).task_list is None


def test_task_list_falls_back_to_workflow_attribute():
    assert WorkflowTask(None, TaskListWorkflow).task_list == "from_attribute"


def test_task_list_falls_back_to_get_task_list():
    assert WorkflowTask(None, GetTaskListWorkflow).task_list == "from_get_task_list"


def test_explicit_task_list_wins_over_get_task_list():
    wf_task = WorkflowTask(None, GetTaskListWorkflow, workflow_task_list="explicit")
    assert wf_task.task_list == "explicit"


def test_explicit_task_list_wins_over_workflow_attribute():
    wf_task = WorkflowTask(None, TaskListWorkflow, workflow_task_list="explicit")
    assert wf_task.task_list == "explicit"


def test_from_generic_task_preserves_explicit_task_list():
    generic = task.WorkflowTask(None, GetTaskListWorkflow, workflow_task_list="explicit")
    swf_task = WorkflowTask.from_generic_task(generic)
    assert isinstance(swf_task, WorkflowTask)
    assert swf_task.task_list == "explicit"


def test_from_generic_task_without_explicit_task_list():
    generic = task.WorkflowTask(None, GetTaskListWorkflow)
    swf_task = WorkflowTask.from_generic_task(generic)
    assert swf_task.task_list == "from_get_task_list"
