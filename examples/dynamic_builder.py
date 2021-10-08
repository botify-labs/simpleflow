from __future__ import print_function

import time

from simpleflow import Activity, Workflow, activity, futures, logger
from simpleflow.canvas import Group, DynamicActivitiesBuilder, Chain
from simpleflow.task import ActivityTask


@activity.with_attributes(task_list="quickstart", version="example")
def increment(x):
    # Here's how you can access the raw context of the activity task if you need
    # it. It gives you access to the response of the PollForActivityTask call to
    # the SWF API. See docs for more info: http://docs.aws.amazon.com/amazonswf/latest/apireference/API_PollForActivityTask.html#API_PollForActivityTask_ResponseSyntax  # NOQA
    logger.warning("activity context: {}".format(increment.context))
    return x + 1


@activity.with_attributes(task_list="quickstart", version="example")
def double(x):
    return x * 2


# simpleflow activities can be classes ; in that case the class is instantiated
# with the params passed via submit, then the `execute()` method is called and
# the result is returned.
@activity.with_attributes(task_list="quickstart", version="example")
class Delay(object):
    def __init__(self, t, x):
        self.t = t
        self.x = x

    def execute(self):
        time.sleep(self.t)
        return self.x


class BasicWorkflow(Workflow):
    name = "basic"
    version = "example"
    task_list = "example"
    tag_list = ["a=1", "b=foo"]

    def run(self, x, t=30):
        execution = self.get_run_context()
        logger.warning("execution context from decider: {}".format(execution))

        metrology = {
            "examples.dynamic_builder.increment": 2 * 60,
            "examples.dynamic_builder.Delay": 10,
            "examples.dynamic_builder.double": 60,
        }
        future = self.submit(
            DynamicActivitiesBuilder(
                metrology,
                Chain(
                    (increment, x),
                    (double, t),
                    (increment, x),
                    (increment, x),
                    (increment, x),
                    raises_on_failure=False,
                )
            )
        )
        print("Result is", future.result)
        return future.result
