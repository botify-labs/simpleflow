import time

from simpleflow import (
    activity,
    Workflow,
    futures,
)


@activity.with_attributes(task_list='quickstart', version='example')
def increment(x):
    return x + 1


@activity.with_attributes(task_list='quickstart', version='example')
def double(x):
    return x * 2


@activity.with_attributes(task_list='quickstart', version='example')
def delay(t, x):
    time.sleep(t)
    return x


@activity.with_attributes(task_list='quickstart', version='example')
def add(x, y):
    return x + y


class BasicWorkflow(Workflow):
    name = 'basic'
    version = 'example'
    task_list = 'example'
    execution_timeout = 60 * 5

    def run(self, x, t=30):
        y = self.submit(increment, x)
        yy = self.submit(delay, t, y)
        z = self.submit(double, y)

        print '({x} + 1) * 2 = {result}'.format(
            x=x,
            result=z.result)
        futures.wait(yy, z)
        return z.result


@activity.with_attributes(task_list='quickstart', version='example')
def fail(x):
    return fail


class FailingWorkflow(Workflow):
    name = 'failing'
    version = 'example'
    task_list = 'example'

    def run(self, x):
        y = self.submit(fail, x)
        z = self.submit(fail, x)
        output = self.submit(add, y, z)
        return output.result
