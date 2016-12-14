from simpleflow import (
    activity,
    Workflow,
)

from . import basic


@activity.with_attributes(task_list='quickstart', version='example')
def add(a, b):
    return a + b


class ComplexWorkflow(Workflow):
    name = 'complex'
    version = 'example'
    task_list = 'example'

    def run(self, x, t=30):
        y = self.submit(basic.BasicWorkflow, x, t)
        z = self.submit(basic.BasicWorkflow, x + 1, t)
        return self.submit(add, y, z).result
