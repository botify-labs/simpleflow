from simpleflow import (
    activity,
    futures,
    Workflow,
)

# This file demonstrates ability to handle Child Workflows with simpleflow.
# Basically it launches a ParentWorkflow that triggers a ChildWorkflow in
# the middle of the process.

@activity.with_attributes(task_list='quickstart', version='example')
def loudly_increment(x, whoami):
    result = x + 1
    print "I am {} and I'll increment x={} : result={}".format(whoami, x, result)
    return result


class ChildWorkflow(Workflow):
    name = 'basic'
    version = 'example'
    task_list = 'example'

    def run(self, x):
        y = self.submit(loudly_increment, x, "CHILD")
        z = self.submit(loudly_increment, y, "CHILD")
        return z.result


class ParentWorkflow(Workflow):
    name = 'basic'
    version = 'example'
    task_list = 'example'

    def run(self, x):
        y = self.submit(loudly_increment, x, "PARENT")
        z = self.submit(ChildWorkflow, y)
        t = self.submit(loudly_increment, z, "PARENT")
        print "Final result should be: {} + 4 = {}".format(x, t.result)
        return t.result
