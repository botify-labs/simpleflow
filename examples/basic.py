import time

from simpleflow import (
    activity,
    Workflow,
)


@activity.with_attributes(task_list='quickstart')
def increment(x):
    return x + 1


@activity.with_attributes(task_list='quickstart')
def double(x):
    return x * 2


@activity.with_attributes(task_list='quickstart', version='example')
def delay(t, x):
    time.sleep(t)
    return x


class BasicWorkflow(Workflow):
    name = 'basic'
    version = 'example'

    def run(self, x, t=30):
        y = self.submit(increment, x)
        yy = self.submit(delay, t, y)
        z = self.submit(double, yy)

        print '({x} + 1) * 2 = {result}'.format(
            x=x,
            result=z.result)
        return z.result
