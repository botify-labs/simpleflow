import time

from simpleflow import (
    activity,
    Workflow,
    futures,
)


@activity.with_attributes(task_list='quickstart', version='example',
                          start_to_close_timeout=60, heartbeat_timeout=15,
                          raises_on_failure=True)
def sleep():
    print "will sleep"
    time.sleep(30)
    print "good sleep"

class SleepWorkflow(Workflow):
    name = 'basic'
    version = 'example'
    task_list = 'example'

    def run(self):
        x = self.submit(sleep)
        futures.wait(x)
