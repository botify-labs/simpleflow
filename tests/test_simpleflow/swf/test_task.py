from __future__ import annotations

from sure import expect

from simpleflow import activity
from simpleflow.swf.task import ActivityTask


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
