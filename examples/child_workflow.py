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


@activity.with_attributes(task_list='quickstart', version='example')
def loudly_increment(x, whoami):
    # WARNING? PARENT executed on example, CHILD on quickstart
    result = x + 1
    print("I am {} and I'll increment x={} : result={}".format(whoami, x, result))
    return result


class ChildWorkflow(Workflow):
    name = 'basic_child'
    version = 'example'
    task_list = 'example'
    execution_timeout = 60 * 5

    @classmethod
    def get_workflow_id(cls, *args, **kwargs):
        return kwargs.get('workflow_name', None)

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

    def run(self, x):
        # A different workflow name or id is necessary
        y = self.submit(ChildWorkflow, x, name='SUB-CHILD', workflow_name='sub_child')
        return y.result + randrange(1000000)


class ChildWorkflowWithGetId(Workflow):
    name = 'another_child'
    version = 'example'
    task_list = 'example'
    execution_timeout = 60 * 5

    @classmethod
    def get_workflow_id(cls, *args, **kwargs):
        return kwargs['id']

    def run(self, id):
        print('id={}'.format(id))


class ParentWorkflow(Workflow):
    name = 'basic_parent'
    version = 'example'
    task_list = 'example'

    def run(self, x=1):
        y = self.submit(loudly_increment, x, "PARENT")
        z = self.submit(ChildWorkflow, y)
        z1 = self.submit(IdempotentChildWorkflow, y)
        z2 = self.submit(IdempotentChildWorkflow, y)
        t = self.submit(loudly_increment, z, "PARENT")
        cwwi = self.submit(ChildWorkflowWithGetId, id='child-workflow-43')
        futures.wait(cwwi)
        print("IdempotentChildWorkflow 1: {}; IdempotentChildWorkflow 2: {}".format(z1.result, z2.result))
        print("Final result should be: {} + 4 = {}".format(x, t.result))
        return t.result
