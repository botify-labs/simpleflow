import time

from simpleflow import (
    activity,
    Workflow,
    futures,
)


@activity.with_attributes(task_list='quickstart', version='example',
                          start_to_close_timeout=60, heartbeat_timeout=15,
                          raises_on_failure=True)
def sleep(seconds):
    print "will sleep {}s".format(seconds)
    time.sleep(seconds)
    print "good sleep"
    return "slept {}s".format(seconds)

class SleepWorkflow(Workflow):
    name = 'basic'
    version = 'example'
    task_list = 'example'

    def run(self, seconds):
        x = self.submit(sleep, seconds)
        futures.wait(x)
