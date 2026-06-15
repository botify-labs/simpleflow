from __future__ import annotations

from sure import expect

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
    expect(ActivityTask(show_context_func).execute()).to.be.none
    expect(ActivityTask(ShowContextCls).execute()).to.be.none


def test_task_attaches_context_to_functions():
    ctx = {"foo": "bar"}
    expect(ActivityTask(show_context_func, context=ctx).execute()).to.equal(ctx)
    expect(show_context_func.context).to.equal(ctx)


def test_task_attaches_context_to_object_instances():
    ctx = {"foo": "bar"}
    expect(ActivityTask(ShowContextCls, context=ctx).execute()).to.equal(ctx)
    expect(ShowContextCls.context).to.be.none


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
    expect(WorkflowTask(None, PlainWorkflow).task_list).to.be.none


def test_task_list_falls_back_to_workflow_attribute():
    expect(WorkflowTask(None, TaskListWorkflow).task_list).to.equal("from_attribute")


def test_task_list_falls_back_to_get_task_list():
    expect(WorkflowTask(None, GetTaskListWorkflow).task_list).to.equal("from_get_task_list")


def test_explicit_task_list_wins_over_get_task_list():
    wf_task = WorkflowTask(None, GetTaskListWorkflow, workflow_task_list="explicit")
    expect(wf_task.task_list).to.equal("explicit")


def test_explicit_task_list_wins_over_workflow_attribute():
    wf_task = WorkflowTask(None, TaskListWorkflow, workflow_task_list="explicit")
    expect(wf_task.task_list).to.equal("explicit")


def test_from_generic_task_preserves_explicit_task_list():
    generic = task.WorkflowTask(None, GetTaskListWorkflow, workflow_task_list="explicit")
    swf_task = WorkflowTask.from_generic_task(generic)
    expect(swf_task).to.be.a(WorkflowTask)
    expect(swf_task.task_list).to.equal("explicit")


def test_from_generic_task_without_explicit_task_list():
    generic = task.WorkflowTask(None, GetTaskListWorkflow)
    swf_task = WorkflowTask.from_generic_task(generic)
    expect(swf_task.task_list).to.equal("from_get_task_list")
