from __future__ import print_function
import time
import uuid

from simpleflow import (
    activity,
    Workflow,
    futures,
)
from simpleflow.canvas import Chain, Group
from simpleflow.constants import HOUR, MINUTE
from simpleflow.task import ActivityTask


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


@activity.with_attributes(task_list='quickstart', version='example', idempotent=True)
def get_uuid(unused=None):
    return str(uuid.uuid4())


@activity.with_attributes(task_list='quickstart', version='example')
def increment(x):
    print("increment: %d" % x)
    return x + 1


@activity.with_attributes(task_list='quickstart', version='example')
def double(y):
    print("double: %d" % y)
    return y * 2


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
    decision_tasks_timeout = 5 * MINUTE
    execution_timeout = 1 * HOUR

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


class ASignalingTestParentWorkflow(Workflow):
    name = 'basic'
    version = 'example'
    task_list = 'example'
    decision_tasks_timeout = '300'
    execution_timeout = '3600'

    def run(self, wait_after_first):
        sig = self.submit(self.signal('signal', 1))
        if wait_after_first:
            futures.wait(sig)
        else:
            # Will be signaled once anyway since signals are marked as idempotent
            pass
        sig = self.submit(self.signal('signal', 1))
        futures.wait(self.submit(self.record_marker('marker 1')))
        sig_again = self.submit(self.signal('signal', 3))
        futures.wait(self.submit(self.record_marker('marker 2')))
        sig_ter = self.submit(self.signal('signal', 8, foo='bar'))
        futures.wait(self.submit(self.record_marker('marker 3')))
        futures.wait(sig, sig_again, sig_ter)


class MarkerWorkflow(Workflow):
    name = 'basic'
    version = 'example'
    task_list = 'example'
    decision_tasks_timeout = 5 * MINUTE
    execution_timeout = 1 * HOUR

    def run(self, use_chain):
        m1 = (self.record_marker('marker 1'))
        m2 = (self.record_marker('marker 1', 'some details'))
        m3 = self.record_marker('marker 2', "2nd marker's details")
        if use_chain:
            # Markers will be submitted in 3 replays
            future = self.submit(Chain(
                m1, m2, m3
            ))
        else:
            # Markers will be submitted as one decision
            future = self.submit(m1)
            self.submit(m2)
            self.submit(m3)
        futures.wait(future)


class ChainTestWorkflow(Workflow):
    name = 'chaintest'
    version = 'example'
    task_list = 'example'

    def run(self, x=5):
        future = self.submit(
            Chain(
                ActivityTask(increment, x),
                ActivityTask(double),
                send_result=True
            )
        )
        print('Future: {}'.format(future))
        futures.wait(future)
        print('Result: {}'.format(future.result))  # future.result == [6, 12]

        return future.result


class TestRunChild(Workflow):
    """
    Test the deciders' task list doesn't override the workers' one.
    """
    name = 'example'
    version = 'example'
    task_list = 'example'
    decision_tasks_timeout = 5 * MINUTE
    execution_timeout = 1 * HOUR

    def run(self):
        future = self.submit(ChainTestWorkflow)
        print('Result: {}'.format(future.result))
        return future.result


class TimerWorkflow(Workflow):
    name = 'example'
    version = 'example'
    task_list = 'example'

    def run(self, t1=2, t2=120):
        """
        Cancel timer 2 after timer 1 is fired.
        """
        future = self.submit(
            Group(
                self.start_timer("timer 2", t2),
                Chain(
                    self.start_timer("timer 1", t1),
                    self.cancel_timer("timer 2"),
                ),
            )
        )
        if future.pending:
            print('Starting timers')
        futures.wait(future)
        print('Timer fired, exiting')
