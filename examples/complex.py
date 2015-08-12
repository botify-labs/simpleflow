import time

from simpleflow import (
    activity,
    Workflow,
    futures,
)

from . import basic


class ComplexWorkflow(Workflow):
    name = 'complex'
    version = 'example'
    task_list = 'example'

    def run(self, x, t=30):
        y = self.submit(basic.BasicWorkflow, x, t)
        z = self.submit(basic.BasicWorkflow, x + 1, t)
        return self.submit(basic.add, y, z).result
