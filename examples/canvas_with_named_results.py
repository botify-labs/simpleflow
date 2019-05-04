from __future__ import print_function

import time

from simpleflow import (
    activity,
    futures,
    Workflow,
)
from simpleflow.canvas import Group, Chain
from simpleflow.task import ActivityTask


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
                    ActivityTask(increment_slowly, x),
                    ActivityTask(increment_slowly, y),
                    ActivityTask(increment_slowly, z),
                    canvas_result_label="increment_group",
                    # FIXME should use_canvas_result_label be inherited?
                    # use_canvas_result_label=True,
                ),
                ActivityTask(multiply, canvas_result_label="multiply"),
                send_result=True,
                use_canvas_result_label=True,
            )
        )
        futures.wait(future)

        res = future.result["multiply"]

        print('({}+1)*({}+1)*({}+1) = {}'.format(x, y, z, res))

        # Canvas's and Group's can also be "optional"
        future = self.submit(
            Chain(
                ActivityTask(fail_incrementing, x, canvas_result_label="fail_incrementing"),
                ActivityTask(increment_slowly, 1, canvas_result_label="increment_slowly"),  # never executed
                ActivityTask(multiply, [3, 2], canvas_result_label="multiply"),
                raises_on_failure=False,
                use_canvas_result_label=True,
            )
        )

        assert {"fail_incrementing": None} == future.result, 'Unexpected result {!r}'.format(future.result)
        print('Chain with failure: {}'.format(future.result))

        # Breaking the chain on failure is the default but can be bypassed
        future = self.submit(
            Chain(
                ActivityTask(fail_incrementing, x, canvas_result_label="fail_incrementing"),
                ActivityTask(increment_slowly, 1, canvas_result_label="increment_slowly"),  # executed
                ActivityTask(multiply, [3, 2], canvas_result_label="multiply"),
                break_on_failure=False,
                use_canvas_result_label=True,
            )
        )

        assert {
                   "fail_incrementing": None, "increment_slowly": 2, "multiply": 6
               } == future.result, 'Unexpected result {!r}'.format(future.result)
        print('Chain ignoring failure: {}'.format(future.result))

        # Failing inside a chain by default don't stop an upper chain
        future = self.submit(
            Chain(
                Chain(
                    ActivityTask(fail_incrementing, x, canvas_result_label="fail_incrementing"),
                    raises_on_failure=False,
                    use_canvas_result_label=True,
                ),
                ActivityTask(increment_slowly, 1, canvas_result_label="increment_slowly"),  # executed
                ActivityTask(multiply, [3, 2], canvas_result_label="multiply"),
                use_canvas_result_label=True,
            )
        )

        assert {
                   "result#0": {"fail_incrementing": None}, "increment_slowly": 2, "multiply": 6
               } == future.result, 'Unexpected result {!r}'.format(future.result)
        print('Chain with failure in subchain: {}'.format(future.result))

        # But it can, too
        future = self.submit(
            Chain(
                Chain(
                    Chain(
                        ActivityTask(fail_incrementing, x, canvas_result_label="fail_incrementing"),
                        raises_on_failure=False,
                        bubbles_exception_on_failure=True,
                        use_canvas_result_label=True,
                    ),
                    ActivityTask(increment_slowly, 1, canvas_result_label="increment_slowly"),  # not executed
                    bubbles_exception_on_failure=False,
                    use_canvas_result_label=True,
                ),
                ActivityTask(multiply, [3, 2], canvas_result_label="multiply"),  # executed
                use_canvas_result_label=True,
            )
        )

        assert {
                   "result#0": {"result#0": {"fail_incrementing": None}}, "multiply": 6
               } == future.result, 'Unexpected result {!r}'.format(
            future.result)
        print('Chain with failure in sub-subchain: {}'.format(future.result))

        print('Finished!')


if __name__ == "__main__":
    from simpleflow.local.executor import Executor

    executor = Executor(CanvasWorkflow)
    executor.run()
