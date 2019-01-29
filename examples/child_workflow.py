from __future__ import print_function

from random import randrange

from simpleflow import (
    activity,
    Workflow,
    futures,
)


# This file demonstrates handling Child Workflows with simpleflow.
# It launches a ParentWorkflow that runs a ChildWorkflow, "two" IdempotentChildWorkflow
# and a ChildWorkflowWithGetId. The IdempotentChildWorkflow also runs ChildWorkflow.

# Notes:
#
# * defining workflow ids is automatic for idempotent workflows and handled to the class otherwise:
#    a `get_workflow_id(*args, **kwargs)` class method returning the full workflow id
#
# * similarly, the WF can define a `get_tag_list(*args, **kwargs)` class method


@activity.with_attributes(task_list='quickstart', version='example')
def loudly_increment(x, whoami):
    result = x + 1
    print("I am {} and I'll increment x={} : result={}".format(whoami, x, result))
    return result


class ChildWorkflow(Workflow):
    name = 'basic_child'
    version = 'example'
    task_list = 'example'
    execution_timeout = 60 * 5

    @classmethod
    def get_tag_list(cls, *args, **kwargs):
        return kwargs.get('my_tag_list', None)

    def run(self, x, name="CHILD", **kwargs):
        y = self.submit(loudly_increment, x, name)
        z = self.submit(loudly_increment, y, name)
        return z.result


class IdempotentChildWorkflow(Workflow):
    name = 'basic_idempotent_child'
    version = 'example'
    task_list = 'example'
    execution_timeout = 60 * 5
    idempotent = True

    tag_list = Workflow.INHERIT_TAG_LIST

    def run(self, x):
        y = self.submit(ChildWorkflow, x, name='GRAND-CHILD', my_tag_list=['abc', 'def=3'])
        return y.result + randrange(1000000)


class ChildWorkflowWithGetId(Workflow):
    name = 'another_child'
    version = 'example'
    task_list = 'example'
    execution_timeout = 60 * 5

    @classmethod
    def get_workflow_id(cls, *args, **kwargs):
        return kwargs.get('my_id')

    def run(self, my_id=None):
        print('ChildWorkflowWithGetId: id={}, workflow_id={}'.format(
            my_id, self.get_run_context().get('workflow_id')
        ))


class ParentWorkflow(Workflow):
    name = 'basic_parent'
    version = 'example'
    task_list = 'example'
    tag_list = ['these', 'are', 'tags']

    def __init__(self, executor):
        super(ParentWorkflow, self).__init__(executor)
        self._futures = []

    def submit(self, submittable, *args, **kwargs):
        future = super(ParentWorkflow, self).submit(submittable, *args, **kwargs)
        self._futures.append(future)
        return future

    def wait_all(self):
        futures.wait(*self._futures)

    def run(self, x=1):
        self._futures = []
        y = self.submit(loudly_increment, x, "PARENT")
        z = self.submit(ChildWorkflow, y)
        self.submit(ChildWorkflow, y)
        t = self.submit(loudly_increment, z, "PARENT")
        u = self.submit(IdempotentChildWorkflow, y)
        v = self.submit(IdempotentChildWorkflow, y)
        self.submit(ChildWorkflowWithGetId, my_id='child-workflow-43')
        self.submit(ChildWorkflowWithGetId)
        self.wait_all()
        print("IdempotentChildWorkflow should be: {} = {}".format(u.result, v.result))
        print("Final result should be: {} + 4 = {}".format(x, t.result))
        return t.result
