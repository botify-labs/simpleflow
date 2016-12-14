from __future__ import print_function
import time
import uuid

from simpleflow import (
    activity,
    Workflow,
    futures,
)


@activity.with_attributes(task_list='quickstart', version='example',
                          start_to_close_timeout=60, heartbeat_timeout=15,
                          raises_on_failure=True)
def sleep(seconds):
    print("will sleep {}s".format(seconds))
    time.sleep(seconds)
    print("good sleep")
    # return a complex object so we can visually test the json-ified version of
    # it is displayed in "simpleflow activity.rerun" ; unfortunately hard to
    # include in a unit or integration test...
    return {"result": "slept {}s".format(seconds)}


class SleepWorkflow(Workflow):
    name = 'basic'
    version = 'example'
    task_list = 'example'

    def run(self, seconds):
        x = self.submit(sleep, seconds)
        futures.wait(x)


class ATestDefinitionWithIdempotentTask(Workflow):
    name = 'test_idempotent_workflow'
    version = 'example'
    task_list = 'example'
    decision_tasks_timeout = '300'
    execution_timeout = '3600'

    def run(self):
        results = [self.submit(get_uuid) for _ in range(10)]
        results.append(
            self.submit(get_uuid, results[0].result)  # Changed arguments, must submit
        )
        futures.wait(*results)
        assert all(r.result == results[0].result for r in results[1:-1])
        assert results[0].result != results[-1].result


@activity.with_attributes(task_list='quickstart', version='example', idempotent=True)
def get_uuid(unused=None):
    return str(uuid.uuid4())
