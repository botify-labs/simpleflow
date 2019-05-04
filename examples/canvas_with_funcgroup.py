from __future__ import print_function
import random

from simpleflow import activity, Workflow
from simpleflow.canvas import Chain, FuncGroup, Group


@activity.with_attributes(task_list='example', version='example')
def random_value():
    return random.randint(1, 10)


@activity.with_attributes(task_list='example', version='example')
def no_worry():
    return "Don't worry!"


@activity.with_attributes(task_list='example', version='example')
def panic():
    return "oh oh."


class MyWorkflow(Workflow):
    name = 'canvas'
    version = 'example'
    task_list = 'example'

    def run(self):
        future = self.submit(
            Chain(
                random_value,
                FuncGroup(
                    lambda result: Group(no_worry) if result < 5 else Group(*([panic] * result))
                ),
                send_result=True,
            )
        )
        print(future.result)


if __name__ == "__main__":
    from simpleflow.local.executor import Executor

    executor = Executor(MyWorkflow)
    executor.run()
