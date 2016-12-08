from __future__ import print_function

import time

from simpleflow import (
    activity,
    Workflow,
    futures,
)


@activity.with_attributes(task_list='quickstart', version='example')
def increment(x):
    # Here's how you can access the raw context of the activity task if you need
    # it. It gives you access to the response of the PollForActivityTask call to
    # the SWF API. See docs for more info: http://docs.aws.amazon.com/amazonswf/latest/apireference/API_PollForActivityTask.html#API_PollForActivityTask_ResponseSyntax  # NOQA
    print("DEBUG: activity context: {}".format(increment.context))
    return x + 1


@activity.with_attributes(task_list='quickstart', version='example')
def double(x):
    return x * 2


# simpleflow activities can be classes ; in that case the class is instantiated
# with the params passed via submit, then the `execute()` method is called and
# the result is returned.
@activity.with_attributes(task_list='quickstart', version='example')
class Delay(object):
    def __init__(self, t, x):
        self.t = t
        self.x = x

    def execute(self):
        time.sleep(self.t)
        return self.x


class BasicWorkflow(Workflow):
    name = 'basic'
    version = 'example'
    task_list = 'example'

    def run(self, x, t=30):
        execution = self.get_execution_context()
        print('DEBUG: execution context: {}'.format(execution))
        y = self.submit(increment, x)
        yy = self.submit(Delay, t, y)
        z = self.submit(double, y)

        print('({x} + 1) * 2 = {result}'.format(
            x=x,
            result=z.result))
        futures.wait(yy, z)
        return z.result
