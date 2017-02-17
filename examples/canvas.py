from __future__ import print_function

import time

from simpleflow import (
    activity,
    futures,
    Workflow,
)
from simpleflow.canvas import Group, Chain


@activity.with_attributes(task_list='example', version='example')
def increment_slowly(x):
    time.sleep(1)
    return x + 1


@activity.with_attributes(task_list='example', version='example')
def multiply(numbers):
    val = 1
    for n in numbers:
        val *= n
    return val


@activity.with_attributes(task_list='example', version='example')
def fail_incrementing(_):
    raise ValueError("Failure on CPU intensive operation '+'")


# This workflow demonstrates the use of simpleflow's Chains and Groups
#
# A `Group` wraps a list of tasks that can be executed in parallel. It
# returns a future that is considered "finished" only once ALL the tasks
# in the group are finished.
#
# A `Chain` wraps a list of tasks that need to be executed sequentially.
# As groups, it returns a future that is considered "finished" only
# when all the tasks inside the Chain are finished.
class CanvasWorkflow(Workflow):
    name = 'canvas'
    version = 'example'
    task_list = 'example'

    def run(self):
        x = 1
        y = 2
        z = 3
        future = self.submit(
            Chain(
                Group(
                    (increment_slowly, x),
                    (increment_slowly, y),
                    (increment_slowly, z),
                ),
                multiply,
                send_result=True
            )
        )
        futures.wait(future)

        res = future.result[-1]

        print('({}+1)*({}+1)*({}+1) = {}'.format(x, y, z, res))

        # Canvas's and Group's can also be "optional"
        future = self.submit(
            Chain(
                (fail_incrementing, x),
                (increment_slowly, 1),  # never executed
                (multiply, [2]),
                raises_on_failure=False,
            )
        )

        futures.wait(future)

        # Failing inside a chain doesn't stop a upper chain
        future = self.submit(
            Chain(
                Chain(
                    (fail_incrementing, x),
                    raises_on_failure=False,
                ),
                (increment_slowly, 1),  # executed
                (multiply, [2]),
            )
        )

        futures.wait(future)

        print('SUCCESS!')
