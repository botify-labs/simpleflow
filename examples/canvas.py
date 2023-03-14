import time

from simpleflow import Workflow, activity, futures
from simpleflow.canvas import Chain, ChainFuture, Group, GroupFuture


@activity.with_attributes(task_list="example", version="example")
def increment_slowly(x):
    time.sleep(1)
    return x + 1


@activity.with_attributes(task_list="example", version="example")
def multiply(numbers):
    val = 1
    for n in numbers:
        val *= n
    return val


@activity.with_attributes(task_list="example", version="example")
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
    name = "canvas"
    version = "example"
    task_list = "example"

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
                send_result=True,
            )
        )
        futures.wait(future)

        res = future.result[-1]

        print(f"({x}+1)*({y}+1)*({z}+1) = {res}")

        # Canvas's and Group's can also be "optional"
        future = self.submit(
            Chain(
                (fail_incrementing, x),
                (increment_slowly, 1),  # never executed
                (multiply, [3, 2]),
                raises_on_failure=False,
            )
        )

        assert [None] == future.result, f"Unexpected result {future.result!r}"
        print(f"Chain with failure: {future.result}")

        # Breaking the chain on failure is the default but can be bypassed
        future = self.submit(
            Chain(
                (fail_incrementing, x),
                (increment_slowly, 1),  # executed
                (multiply, [3, 2]),
                break_on_failure=False,
            )
        )

        assert [None, 2, 6] == future.result, "Unexpected result {!r}".format(future.result)
        print(f"Chain ignoring failure: {future.result}")

        # Failing inside a chain by default don't stop an upper chain
        future = self.submit(
            Chain(
                Chain(
                    (fail_incrementing, x),
                    raises_on_failure=False,
                ),
                (increment_slowly, 1),  # executed
                (multiply, [3, 2]),
            )
        )

        assert [[None], 2, 6] == future.result, "Unexpected result {!r}".format(future.result)
        print(f"Chain with failure in subchain: {future.result}")

        # But it can, too
        future = self.submit(
            Chain(
                Chain(
                    Chain(
                        (fail_incrementing, x),
                        raises_on_failure=False,
                        bubbles_exception_on_failure=True,
                    ),
                    (increment_slowly, 1),  # not executed
                    bubbles_exception_on_failure=False,
                ),
                (multiply, [3, 2]),  # executed
            )
        )

        assert [[[None]], 6] == future.result, "Unexpected result {!r}".format(future.result)
        print(f"Chain with failure in sub-subchain: {future.result}")

        print("Finished!")


class CustomGroupFuture(GroupFuture):
    def __init__(self, *args, **kwargs):
        print(f"CustomGroupFuture.__init__({args}, {kwargs})")
        super().__init__(*args, **kwargs)

    @property
    def result(self):
        print("CustomGroupFuture.result: start")
        try:
            result = super().result
        except Exception as ex:
            print(f"CustomGroupFuture.result: exception {ex!r}")
            raise
        print(f"CustomGroupFuture.result: returning {result!r}")
        return result


class CustomGroup(Group):
    default_future_class = CustomGroupFuture


class CustomChainFuture(ChainFuture):
    def __init__(self, *args, **kwargs):
        print(f"CustomChainFuture.__init__({args}, {kwargs})")
        super().__init__(*args, **kwargs)

    @property
    def result(self):
        print("CustomChainFuture.result: start")
        try:
            result = super().result
        except Exception as ex:
            print(f"CustomChainFuture.result: exception {ex!r}")
            raise
        print(f"CustomChainFuture.result: returning {result!r}")
        return result


class CustomChain(Chain):
    default_future_class = CustomChainFuture


class CustomCanvasWorkflow(Workflow):
    name = "canvas"
    version = "example"
    task_list = "example"

    def run(self):
        x, y, z = 1, 2, 3
        fut = self.submit(
            CustomGroup(
                (increment_slowly, x),
                (increment_slowly, y),
                (increment_slowly, z),
                CustomChain(
                    (fail_incrementing, x),
                    raises_on_failure=False,
                ),
            )
        )
        print(f"Result: {fut.result}")
