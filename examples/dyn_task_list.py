from __future__ import print_function

from simpleflow import Workflow, activity


def run_on(func, task_list, **kwargs):
    return activity.with_attributes(task_list=task_list, **kwargs)(func)


def double(x):
    return x * 2


class BasicWorkflow(Workflow):
    name = "basic"
    version = "example"

    @classmethod
    def get_task_list(cls, task_list, *args, **kwargs):
        return task_list

    def run(self, x, task_list, *args, **kwargs):
        result = self.submit(run_on(double, task_list), x).result
        print("Result: {}".format(result))
