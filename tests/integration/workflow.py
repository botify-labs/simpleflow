from __future__ import annotations

import time
import uuid

from simpleflow import Workflow, activity, futures
from simpleflow.canvas import Chain, Group
from simpleflow.constants import HOUR, MINUTE
from simpleflow.swf.utils import get_workflow_execution
from simpleflow.task import ActivityTask


@activity.with_attributes(
    task_list="quickstart",
    version="example",
    start_to_close_timeout=60,
    heartbeat_timeout=15,
    raises_on_failure=True,
)
def sleep(seconds):
    print(f"will sleep {seconds}s")
    time.sleep(seconds)
    print("good sleep")
    # return a complex object so we can visually test the json-ified version of
    # it is displayed in "simpleflow activity.rerun" ; unfortunately hard to
    # include in a unit or integration test...
    return {"result": f"slept {seconds}s"}


@activity.with_attributes(task_list="quickstart", version="example", idempotent=True)
def get_uuid(unused=None):
    return str(uuid.uuid4())


@activity.with_attributes(task_list="quickstart", version="example")
def increment(x):
    print("increment: %d" % x)
    return x + 1


@activity.with_attributes(task_list="quickstart", version="example")
def double(y):
    print("double: %d" % y)
    return y * 2


@activity.with_attributes(task_list="quickstart", version="example")
def send_unrequested_signal():
    context = send_unrequested_signal.context
    ex = get_workflow_execution(context["domain_name"], context["workflow_id"], context["run_id"])
    ex.connection.signal_workflow_execution(
        ex.domain.name,
        "unexpected",
        ex.workflow_id,
        input="Hi there!",  # not JSON-formatted
        run_id=ex.run_id,
    )
    return "signal sent!"


@activity.with_attributes(task_list="quickstart", version="example")
def cancel_workflow():
    context = cancel_workflow.context
    workflow_id = context["workflow_id"]
    run_id = context["run_id"]
    domain_name = context["domain_name"]
    workflow_execution = get_workflow_execution(domain_name, workflow_id, run_id)
    workflow_execution.request_cancel()


class SleepWorkflow(Workflow):
    name = "basic"
    version = "example"
    task_list = "example"

    def run(self, seconds):
        x = self.submit(sleep, seconds)
        futures.wait(x)


class ATestDefinitionWithIdempotentTask(Workflow):
    name = "test_idempotent_workflow"
    version = "example"
    task_list = "example"
    decision_tasks_timeout = 5 * MINUTE
    execution_timeout = 1 * HOUR

    def run(self):
        results = [self.submit(get_uuid) for _ in range(10)]
        results.append(self.submit(get_uuid, results[0].result))  # Changed arguments, must submit
        futures.wait(*results)
        assert all(r.result == results[0].result for r in results[1:-1])
        assert results[0].result != results[-1].result


class MarkerWorkflow(Workflow):
    name = "example"
    version = "example"
    task_list = "example"
    decision_tasks_timeout = 5 * MINUTE
    execution_timeout = 1 * HOUR

    def run(self, use_chain):
        m1 = self.record_marker("marker 1")
        m2 = self.record_marker("marker 1", "some details")
        m3 = self.record_marker("marker 2", "2nd marker's details")
        if use_chain:
            # Markers will be submitted in 3 replays
            future = self.submit(Chain(m1, m2, m3))
        else:
            # Markers will be submitted as one decision
            future = self.submit(m1)
            self.submit(m2)
            self.submit(m3)
        futures.wait(future)


class ChainTestWorkflow(Workflow):
    name = "chaintest"
    version = "example"
    task_list = "example"

    def run(self, x=5):
        future = self.submit(Chain(ActivityTask(increment, x), ActivityTask(double), send_result=True))
        print(f"Future: {future}")
        futures.wait(future)
        print(f"Result: {future.result}")  # future.result == [6, 12]

        return future.result


class TestRunChild(Workflow):
    """
    Test the deciders' task list doesn't override the workers' one.
    """

    name = "example"
    version = "example"
    task_list = "example"
    decision_tasks_timeout = 5 * MINUTE
    execution_timeout = 1 * HOUR

    def run(self):
        future = self.submit(ChainTestWorkflow)
        print(f"Result: {future.result}")
        return future.result


class TimerWorkflow(Workflow):
    name = "example"
    version = "example"
    task_list = "example"

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
            print("Starting timers")
        futures.wait(future)
        print("Timer fired, exiting")


class SignaledWorkflow(Workflow):
    name = "example"
    version = "example"
    task_list = "example"
    decision_tasks_timeout = 5 * MINUTE
    execution_timeout = 1 * HOUR

    def run(self):
        future = self.submit(send_unrequested_signal)
        return future.result


class WorkflowToCancel(Workflow):
    name = "example"
    version = "example"
    task_list = "example"

    def run(self, *args, **kwargs):
        future = self.submit(cancel_workflow)
        return future.result

    def should_cancel(self, history):
        input = history.events[0].input or {}
        if input.get("args"):
            agree = input["args"][0]
        else:
            agree = input.get("kwargs", {}).get("agree", True)
        print(f"should_cancel called! agree? {agree}")
        return agree


@activity.with_attributes(task_list="quickstart", version="example")
def wait_and_signal(name="signal"):
    time.sleep(1 + len(name))  # Hoping to be deterministic
    context = wait_and_signal.context
    ex = get_workflow_execution(context["domain_name"], context["workflow_id"], context["run_id"])
    ex.connection.signal_workflow_execution(
        ex.domain.name,
        name,
        ex.workflow_id,
        run_id=ex.run_id,
    )


class GroupTestWorkflowWithChild(Workflow):
    name = "example"
    version = "example"
    task_list = "example"

    def run(self):
        g = Group()
        g.append(ChainTestWorkflow, 4)
        future = self.submit(g)
        return future.result


class WorkflowWithWaitSignal(Workflow):
    name = "example"
    version = "example"
    task_list = "example"

    def run(self, *args, **kwargs):
        future = self.submit(
            Chain(
                Group(
                    (self.wait_signal("signal 2"),),
                    (self.wait_signal("signal"),),
                    (wait_and_signal,),
                    (wait_and_signal, "signal 2"),
                ),
                (increment, 1),
            )
        )
        futures.wait(future)
