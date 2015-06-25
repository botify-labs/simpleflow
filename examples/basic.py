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


class BasicWorkflow(Workflow):
    name = 'basic'
    version = 'example'

    def run(self, x):
        y = self.submit(increment, x)
        z = self.submit(double, y)

        print '({x} + 1) * 2 = {result}'.format(
            x=x,
            result=z.result)
        return z.result
