from __future__ import annotations

import datetime
import functools
from unittest.mock import patch

import boto

import swf.models
import swf.models.decision
import swf.models.workflow
from simpleflow import futures
from simpleflow.history import History
from simpleflow.swf import constants
from simpleflow.swf.executor import Executor
from simpleflow.swf.task import NonPythonicActivityTask
from simpleflow.task import ActivityTask
from simpleflow.utils import json_dumps
from swf.models.history import builder
from swf.responses import Response
from tests.data.activities import (
    Tetra,
    double,
    increment,
    increment_retry,
    non_pythonic,
    print_message,
    raise_error,
    raise_on_failure,
    triple,
)
from tests.data.constants import DOMAIN
from tests.data.workflows import BaseTestWorkflow
from tests.moto_compat import mock_swf


def check_task_scheduled_decision(decision, task):
    """
    Asserts that *decision* schedules *task*.
    """
    assert decision["decisionType"] == "ScheduleActivityTask"

    attributes = decision["scheduleActivityTaskDecisionAttributes"]
    assert attributes["activityType"] == {"name": task.name, "version": task.version}


class ATestDefinitionWithInput(BaseTestWorkflow):
    """
    Execute a single task with an argument passed as the workflow's input.
    """

    def run(self, a):
        b = self.submit(increment, a)
        return b.result


@mock_swf
def test_workflow_with_input():
    workflow = ATestDefinitionWithInput
    executor = Executor(DOMAIN, workflow)

    result = 5
    history = builder.History(workflow, input={"args": (4,)})

    # The executor should only schedule the *increment* task.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], increment)

    # Let's add the task to the history to simulate its completion.
    decision_id = history.last_id
    (
        history.add_activity_task(
            increment,
            decision_id=decision_id,
            last_state="completed",
            activity_id="activity-tests.data.activities.increment-1",
            input={"args": 1},
            result=result,
        )
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # As there is only a single task, the executor should now complete the
    # workflow and set its result accordingly.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    workflow_completed = swf.models.decision.WorkflowExecutionDecision()
    workflow_completed.complete(result=result)

    assert decisions[0] == workflow_completed


class ATestDefinitionThatSubmitsAnActivityTask(BaseTestWorkflow):
    """
    Execute a single task already wrapped as a simpleflow.task.ActivityTask.
    """

    def run(self):
        b = self.submit(ActivityTask(increment, 4))
        return b.result


@mock_swf
def test_workflow_that_submits_an_activity_task():
    workflow = ATestDefinitionThatSubmitsAnActivityTask
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow)

    # The executor should only schedule the *increment* task.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], increment)


@mock_swf
def test_workflow_with_repair_if_task_successful():
    workflow = ATestDefinitionWithInput
    history = builder.History(workflow, input={"args": [4]})

    # Now let's build the history to repair
    previous_history = builder.History(workflow, input={"args": [4]})
    decision_id = previous_history.last_id
    (
        previous_history.add_activity_task(
            increment,
            decision_id=decision_id,
            last_state="completed",
            activity_id="activity-tests.data.activities.increment-1",
            input={"args": 4},
            result=57,
        )  # obviously wrong but helps see if things work
    )
    to_repair = History(previous_history)
    to_repair.parse()

    executor = Executor(DOMAIN, workflow, repair_with=to_repair)

    # The executor should not schedule anything, it should use previous history
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    assert len(decisions) == 1
    assert decisions[0]["decisionType"] == "ScheduleActivityTask"
    attrs = decisions[0]["scheduleActivityTaskDecisionAttributes"]
    assert attrs["taskList"]["name"].startswith("FAKE-")


@mock_swf
def test_workflow_with_repair_if_task_failed():
    workflow = ATestDefinitionWithInput
    history = builder.History(workflow, input={"args": [4]})

    # Now let's build the history to repair
    previous_history = builder.History(workflow, input={"args": [4]})
    decision_id = previous_history.last_id
    (
        previous_history.add_activity_task(
            increment,
            decision_id=decision_id,
            last_state="failed",
            activity_id="activity-tests.data.activities.increment-1",
            input={"args": 4},
            result=57,
        )  # obviously wrong but helps see if things work
    )
    to_repair = History(previous_history)
    to_repair.parse()

    executor = Executor(DOMAIN, workflow, repair_with=to_repair)

    # The executor should not schedule anything, it should use previous history
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], increment)


@mock_swf
def test_workflow_with_repair_and_force_activities():
    workflow = ATestDefinitionWithInput
    history = builder.History(workflow, input={"args": [4]})

    # Now let's build the history to repair
    previous_history = builder.History(workflow, input={"args": [4]})
    decision_id = previous_history.last_id
    (
        previous_history.add_activity_task(
            increment,
            decision_id=decision_id,
            last_state="completed",
            activity_id="activity-tests.data.activities.increment-1",
            input={"args": 4},
            result=57,
        )  # obviously wrong but helps see if things work
    )
    to_repair = History(previous_history)
    to_repair.parse()

    executor = Executor(
        DOMAIN,
        workflow,
        repair_with=to_repair,
        force_activities="increment|something_else",
    )

    # The executor should not schedule anything, it should use previous history
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    assert len(decisions) == 1
    assert decisions[0]["decisionType"] == "ScheduleActivityTask"
    attrs = decisions[0]["scheduleActivityTaskDecisionAttributes"]
    assert not attrs["taskList"]["name"].startswith("FAKE-")
    check_task_scheduled_decision(decisions[0], increment)


class ATestDefinitionWithBeforeReplay(BaseTestWorkflow):
    """
    Execute a single task with an argument passed as the workflow's input.
    """

    def before_replay(self, history):
        self.a = history.events[0].input["args"][0]

    def run(self, a):
        b = self.submit(increment, a)
        return b.result


@mock_swf
@patch.object(Executor, "decref_workflow")
def test_workflow_with_before_replay(mock_decref_workflow):
    workflow = ATestDefinitionWithBeforeReplay
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow, input={"args": (4,)})

    # The executor should only schedule the *increment* task.
    assert not hasattr(executor.workflow, "a")
    executor.replay(Response(history=history, execution=None))
    assert executor.workflow.a == 4


class ATestDefinitionWithAfterReplay(BaseTestWorkflow):
    """
    Execute a single task with an argument passed as the workflow's input.
    """

    def after_replay(self, history):
        self.b = history.events[0].input["args"][0] + 1

    def after_closed(self, history):
        self.c = history.events[0].input["args"][0] + 1

    def run(self, a):
        b = self.submit(increment, a)
        return b.result


@mock_swf
@patch.object(Executor, "decref_workflow")
def test_workflow_with_after_replay(mock_decref_workflow):
    workflow = ATestDefinitionWithAfterReplay
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow, input={"args": (4,)})

    # The executor should only schedule the *increment* task.
    assert not hasattr(executor.workflow, "b")
    _ = executor.replay(Response(history=history, execution=None)).decisions
    assert executor.workflow.b == 5
    # Check that workflow is not marked as finished
    assert not hasattr(executor.workflow, "c")


class ATestDefinitionWithAfterClosed(BaseTestWorkflow):
    """
    Execute a single task with an argument passed as the workflow's input.
    """

    def after_closed(self, history):
        self.b = history.events[0].input["args"][0] + 1

    def run(self, a):
        b = self.submit(increment, a)
        return b.result


@mock_swf
@patch.object(Executor, "decref_workflow")
def test_workflow_with_after_closed(mock_decref_workflow):
    workflow = ATestDefinitionWithAfterClosed
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow, input={"args": (4,)})

    # The executor should only schedule the *increment* task.
    assert not hasattr(executor.workflow, "b")
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], increment)

    # Let's add the task to the history to simulate its completion.
    decision_id = history.last_id
    (
        history.add_activity_task(
            increment,
            decision_id=decision_id,
            last_state="completed",
            activity_id="activity-tests.data.activities.increment-1",
            input={"args": 4},
            result=5,
        )
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # *double* has completed and the ``b.result``is now available. The executor
    # should complete the workflow and its result to ``b.result``.
    assert not hasattr(executor.workflow, "b")
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    workflow_completed = swf.models.decision.WorkflowExecutionDecision()
    workflow_completed.complete(result=5)

    assert decisions[0] == workflow_completed
    assert executor.workflow.b == 5


class ATestDefinition(BaseTestWorkflow):
    """
    Executes two tasks. The second depends on the first.
    """

    def run(self):
        a = self.submit(increment, 1)
        assert isinstance(a, futures.Future)

        b = self.submit(double, a)

        return b.result


@mock_swf
def test_workflow_with_two_tasks():
    workflow = ATestDefinition
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow)

    # *double* requires the result of *increment*, hold by the *a* future.
    # Hence the executor schedule *increment*.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], increment)

    # Let's add the task to the history to simulate its completion.
    decision_id = history.last_id
    (
        history.add_activity_task(
            increment,
            decision_id=decision_id,
            last_state="completed",
            activity_id="activity-tests.data.activities.increment-1",
            input={"args": 1},
            result=2,
        )
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # Now ``a.result``contains the result of *increment*'s that is finished.
    # The line ``return b.result`` requires the computation of *double* with
    # ``a.result``, then the executor should schedule *double*.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], double)

    # Let's add the task to the history to simulate its completion.
    decision_id = history.last_id
    (
        history.add_activity_task(
            double,
            decision_id=decision_id,
            last_state="completed",
            activity_id="activity-tests.data.activities.double-1",
            input={"args": 2},
            result=4,
        )
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # *double* has completed and the ``b.result``is now available. The executor
    # should complete the workflow and its result to ``b.result``.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    workflow_completed = swf.models.decision.WorkflowExecutionDecision()
    workflow_completed.complete(result=4)

    assert decisions[0] == workflow_completed


@mock_swf
def test_workflow_with_two_tasks_not_completed():
    """
    This test checks how the executor behaves when a task is still running.
    """
    workflow = ATestDefinitionWithInput
    executor = Executor(DOMAIN, workflow)

    arg = 4
    result = 5
    history = builder.History(workflow, input={"args": (arg,)})

    # The executor should schedule *increment*.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], increment)

    # Let's add the task in state ``started`` to the history.
    decision_id = history.last_id
    scheduled_id = decision_id + 1
    (
        history.add_activity_task(
            increment,
            decision_id=decision_id,
            last_state="started",
            activity_id="activity-tests.data.activities.increment-1",
            input={"args": 1},
            result=5,
        )
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # The executor cannot schedule any other task, it returns an empty
    # decision.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    assert len(decisions) == 0

    # Let's now set the task as ``completed`` in the history.
    decision_id = history.last_id
    (
        history.add_activity_task_completed(scheduled=scheduled_id, started=scheduled_id + 1, result=result)
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # As there is a single task and it is now finished, the executor should
    # complete the workflow.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    workflow_completed = swf.models.decision.WorkflowExecutionDecision()
    workflow_completed.complete(result=result)

    assert decisions[0] == workflow_completed


class ATestDefinitionSameTask(BaseTestWorkflow):
    """
    This workflow executes the same task with a different argument.
    """

    def run(self, *args, **kwargs):
        a = self.submit(increment, 1)
        b = self.submit(increment, a)

        return b.result


@mock_swf
def test_workflow_with_same_task_called_two_times():
    """
    This test checks how the executor behaves when the same task is executed
    two times with a different argument.
    """
    workflow = ATestDefinitionSameTask
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow)

    # As the second task depends on the first, the executor should only
    # schedule the first task.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], increment)

    # Let's add the task to the history to simulate its completion.
    decision_id = history.last_id
    (
        history.add_activity_task(
            increment,
            decision_id=decision_id,
            last_state="completed",
            activity_id="activity-tests.data.activities.increment-1",
            input={"args": 1},
            result=2,
        )
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # The first task is finished, the executor should schedule the second one.
    decision_id = history.last_id
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], increment)

    # Let's add the task to the history to simulate its completion.
    decision_id = history.last_id
    (
        history.add_activity_task(
            increment,
            decision_id=decision_id,
            last_state="completed",
            activity_id="activity-tests.data.activities.increment-2",
            input={"args": 2},
            result=3,
        )
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # The executor should now complete the workflow.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    workflow_completed = swf.models.decision.WorkflowExecutionDecision()
    workflow_completed.complete(result=3)

    assert decisions[0] == workflow_completed


class ATestDefinitionSameFuture(BaseTestWorkflow):
    """
    This workflow uses a single variable to hold the future of two different
    tasks.
    """

    def run(self, *args, **kwargs):
        a = self.submit(increment, 1)
        a = self.submit(double, a)

        return a.result


@mock_swf
def test_workflow_reuse_same_future():
    workflow = ATestDefinitionSameFuture
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow)

    # *double* depends on *increment*, then the executor should only schedule
    # *increment* at first.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], increment)

    # Let's add the task to the history to simulate its completion.
    decision_id = history.last_id
    (
        history.add_activity_task(
            increment,
            decision_id=decision_id,
            last_state="completed",
            input={"args": 1},
            activity_id="activity-tests.data.activities.increment-1",
            result=2,
        )
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # *increment* is finished, the executor should schedule *double*.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], double)

    # Let's add the task to the history to simulate its completion.
    decision_id = history.last_id
    (
        history.add_activity_task(
            double,
            decision_id=decision_id,
            last_state="completed",
            activity_id="activity-tests.data.activities.double-1",
            input={"args": 2},
            result=4,
        )
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # The executor should now complete the workflow.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    workflow_completed = swf.models.decision.WorkflowExecutionDecision()
    workflow_completed.complete(result=4)

    assert decisions[0] == workflow_completed


class ATestDefinitionTwoTasksSameFuture(BaseTestWorkflow):
    """
    This test checks how the executor behaves when two tasks depends on the
    same task.
    """

    def run(self, *args, **kwargs):
        a = self.submit(increment, 1)
        b = self.submit(double, a)
        c = self.submit(increment, a)

        return (b.result, c.result)


@mock_swf
def test_workflow_with_two_tasks_same_future():
    workflow = ATestDefinitionTwoTasksSameFuture
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow)

    # ``b.result`` and ``c.result`` requires the execution of ``double(a)`` and
    # ``increment(a)``. They both depend on the execution of ``increment(1)``so
    # the executor should schedule ``increment(1)``.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], increment)

    # Let's add the task to the history to simulate its completion.
    decision_id = history.last_id
    (
        history.add_activity_task(
            increment,
            decision_id=decision_id,
            last_state="completed",
            activity_id="activity-tests.data.activities.increment-1",
            input={"args": 1},
            result=2,
        )
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # Now ``a.result`` is available and the executor should schedule the
    # execution of ``double(a)`` and ``increment(a)`` at the same time.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], double)
    check_task_scheduled_decision(decisions[1], increment)

    # Let's add both tasks to the history to simulate their completion.
    decision_id = history.last_id
    (
        history.add_activity_task(
            double,
            decision_id=decision_id,
            last_state="completed",
            activity_id="activity-tests.data.activities.double-1",
            input={"args": 2},
            result=4,
        )
        .add_activity_task(
            increment,
            decision_id=decision_id,
            last_state="completed",
            activity_id="activity-tests.data.activities.increment-2",
            input={"args": 2},
            result=3,
        )
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # Both tasks completed, hence the executor should complete the workflow.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    workflow_completed = swf.models.decision.WorkflowExecutionDecision()
    workflow_completed.complete(result=(4, 3))

    assert decisions[0] == workflow_completed


class ATestDefinitionMap(BaseTestWorkflow):
    """
    This workflow only maps a task on several values, they wait for the
    availability of their result.
    """

    nb_parts = 3

    def run(self, *args, **kwargs):
        xs = self.map(increment, list(range(self.nb_parts)))
        values = futures.wait(*xs)

        return values


@mock_swf
def test_workflow_map():
    workflow = ATestDefinitionMap
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow)

    nb_parts = ATestDefinitionMap.nb_parts

    # All the futures returned by the map are passed to wait().
    # The executor should then schedule all of them.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    for i in range(nb_parts):
        check_task_scheduled_decision(decisions[i], increment)

    # Let's add all tasks of the map to the history to simulate their
    # completion.
    decision_id = history.last_id
    for i in range(nb_parts):
        history.add_activity_task(
            increment,
            decision_id=decision_id,
            activity_id=f"activity-tests.data.activities.increment-{i + 1}",
            last_state="completed",
            input={"args": i},
            result=i + 1,
        )
    (history.add_decision_task_scheduled().add_decision_task_started())

    # All tasks are finished, the executor should complete the workflow.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    workflow_completed = swf.models.decision.WorkflowExecutionDecision()
    workflow_completed.complete(result=[i + 1 for i in range(nb_parts)])

    assert decisions[0] == workflow_completed


class ATestDefinitionRetryActivity(BaseTestWorkflow):
    """
    This workflow executes a task that is retried on failure.
    """

    def run(self, *args, **kwargs):
        a = self.submit(increment_retry, 7)

        return a.result


@mock_swf
def test_workflow_retry_activity():
    workflow = ATestDefinitionRetryActivity
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow)

    # There is a single task, hence the executor should schedule it first.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], increment_retry)

    # Let's add the task in ``failed`` state.
    decision_id = history.last_id
    (
        history.add_activity_task(
            increment_retry,
            decision_id=decision_id,
            last_state="failed",
            activity_id="activity-tests.data.activities.increment_retry-1",
        )
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # As the retry value is one, the executor should retry i.e. schedule the
    # task again.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], increment_retry)

    # Let's add the task in ``completed`` state.
    decision_id = history.last_id
    (
        history.add_activity_task(
            increment_retry,
            decision_id=decision_id,
            last_state="completed",
            activity_id="activity-tests.data.activities.increment_retry-1",
            input={"args": 7},
            result=8,
        )
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # Now the task is finished and the executor should complete the workflow.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    workflow_completed = swf.models.decision.WorkflowExecutionDecision()
    workflow_completed.complete(result=8)

    assert decisions[0] == workflow_completed


@mock_swf
def test_workflow_retry_activity_failed_again():
    workflow = ATestDefinitionRetryActivity
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow)

    # There is a single task, hence the executor should schedule it first.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], increment_retry)

    # Let's add the task in ``failed`` state.
    decision_id = history.last_id
    (
        history.add_activity_task(
            increment_retry,
            decision_id=decision_id,
            last_state="failed",
            activity_id="activity-tests.data.activities.increment_retry-1",
        )
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # As the retry value is one, the executor should retry i.e. schedule the
    # task again.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], increment_retry)

    # Let's add the task in ``failed`` state again.
    decision_id = history.last_id
    (
        history.add_activity_task(
            increment_retry,
            decision_id=decision_id,
            last_state="failed",
            activity_id="activity-tests.data.activities.increment_retry-1",
        )
        .add_decision_task_scheduled()
        .add_decision_task_started()
    )

    # There is no more retry. The executor should set `Future.exception` and
    # complete the workflow as there is no further task.
    decisions = executor.replay(Response(history=history, execution=None)).decisions

    workflow_completed = swf.models.decision.WorkflowExecutionDecision()
    # ``a.result`` is ``None`` because it was not set.
    workflow_completed.complete(result=None)

    assert decisions[0] == workflow_completed


class ATestDefinitionChildWorkflow(BaseTestWorkflow):
    """
    This workflow executes a child workflow.
    """

    def run(self, x):
        y = self.submit(ATestDefinition, x)
        return y.result


@mock_swf
def test_workflow_with_child_workflow():
    workflow = ATestDefinitionChildWorkflow
    executor = Executor(DOMAIN, workflow)

    # FIXME Py3 the original test only contains args, and check both keys are present.
    # FIXME Py3 But dict order is unspecified from one execution to the next
    input = {"args": (1,), "kwargs": {}}
    history = builder.History(workflow, input=input)

    # The executor should schedule the execution of a child workflow.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    assert len(decisions) == 1
    assert decisions == [
        {
            "startChildWorkflowExecutionDecisionAttributes": {
                "workflowId": "workflow-test_workflow-None--0--1",
                "taskList": {"name": "test_task_list"},
                "executionStartToCloseTimeout": "3600",
                "input": json_dumps(input),
                "workflowType": {
                    "version": "test_version",
                    "name": "tests.test_simpleflow.test_dataflow.ATestDefinition",
                },
                "taskStartToCloseTimeout": "300",
            },
            "decisionType": "StartChildWorkflowExecution",
        }
    ]

    # Let's add the child workflow to the history to simulate its completion.
    (
        history.add_decision_task().add_child_workflow(
            workflow,
            workflow_id="workflow-test_workflow-None--0--1",
            task_list=BaseTestWorkflow.task_list,
            input='"{\\"args\\": [1], \\"kwargs\\": {}}"',
            result="4",
        )
    )

    # Now the child workflow is finished and the executor should complete the
    # workflow.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    workflow_completed = swf.models.decision.WorkflowExecutionDecision()
    workflow_completed.complete(result=4)

    assert decisions[0] == workflow_completed


def test_workflow_with_child_workflow_failed():
    workflow = ATestDefinitionChildWorkflow
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow, input={"args": (1,)})

    decisions = executor.replay(Response(history=history, execution=None)).decisions
    # Let's add the child workflow to the history to simulate its completion.
    (
        history.add_child_workflow(
            workflow,
            last_state="failed",
            workflow_id="workflow-test_workflow-None--0--1",
            task_list=BaseTestWorkflow.task_list,
            input='"{\\"args\\": [1], \\"kwargs\\": {}}"',
        )
    )
    # The child workflow fails and the executor should fail the
    # main workflow.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    fail_workflow = swf.models.decision.WorkflowExecutionDecision()
    fail_workflow.fail(reason="FAIL")

    decision = decisions[0]
    assert decision.type == "FailWorkflowExecution"
    reason = decision["failWorkflowExecutionDecisionAttributes"]["reason"]
    assert reason == 'Workflow execution error in workflow-test_workflow: "None"'


def test_workflow_with_child_workflow_timed_out():
    workflow = ATestDefinitionChildWorkflow
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow, input={"args": (1,)})

    decisions = executor.replay(Response(history=history, execution=None)).decisions
    # Let's add the child workflow to the history to simulate its completion.
    (
        history.add_child_workflow(
            workflow,
            last_state="timed_out",
            workflow_id="workflow-test_workflow-None--0--1",
            task_list=BaseTestWorkflow.task_list,
            input='"{\\"args\\": [1], \\"kwargs\\": {}}"',
        )
    )
    # The child workflow fails and the executor should fail the
    # main workflow.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    fail_workflow = swf.models.decision.WorkflowExecutionDecision()
    fail_workflow.fail(reason="timed out")

    decision = decisions[0]
    assert decision.type == "FailWorkflowExecution"
    reason = decision["failWorkflowExecutionDecisionAttributes"]["reason"]
    assert reason == 'Workflow execution error in workflow-test_workflow: "TimeoutError(START_TO_CLOSE)"'


def test_workflow_with_child_workflow_canceled():
    workflow = ATestDefinitionChildWorkflow
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow, input={"args": (1,)})

    decisions = executor.replay(Response(history=history, execution=None)).decisions
    # Let's add the child workflow to the history to simulate its completion.
    (
        history.add_child_workflow(
            workflow,
            last_state="canceled",
            workflow_id="workflow-test_workflow-None--0--1",
            task_list=BaseTestWorkflow.task_list,
            input='"{\\"args\\": [1], \\"kwargs\\": {}}"',
        )
    )
    # The child workflow fails and the executor should fail the
    # main workflow.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    fail_workflow = swf.models.decision.WorkflowExecutionDecision()
    fail_workflow.cancel()

    decision = decisions[0]
    assert decision.type == "FailWorkflowExecution"
    reason = decision["failWorkflowExecutionDecisionAttributes"]["reason"]
    assert reason == 'Workflow execution error in workflow-test_workflow: "TaskCanceled()"'


def test_workflow_with_child_workflow_terminated():
    workflow = ATestDefinitionChildWorkflow
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow, input={"args": (1,)})

    decisions = executor.replay(Response(history=history, execution=None)).decisions
    # Let's add the child workflow to the history to simulate its completion.
    (
        history.add_child_workflow(
            workflow,
            last_state="terminated",
            workflow_id="workflow-test_workflow-None--0--1",
            task_list=BaseTestWorkflow.task_list,
            input='"{\\"args\\": [1], \\"kwargs\\": {}}"',
        )
    )
    # The child workflow fails and the executor should fail the
    # main workflow.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    fail_workflow = swf.models.decision.WorkflowExecutionDecision()
    fail_workflow.terminate()

    decision = decisions[0]
    assert decision.type == "FailWorkflowExecution"
    reason = decision["failWorkflowExecutionDecisionAttributes"]["reason"]
    assert reason == 'Workflow execution error in workflow-test_workflow: "TaskTerminated()"'


class ATestDefinitionMoreThanMaxDecisions(BaseTestWorkflow):
    """
    This workflow executes more tasks than the maximum number of decisions a
    decider can take once.
    """

    def run(self):
        results = self.map(increment, list(range(constants.MAX_DECISIONS + 5)))
        futures.wait(*results)


@mock_swf
def test_workflow_with_more_than_max_decisions():
    workflow = ATestDefinitionMoreThanMaxDecisions
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow)

    # The first time, the executor should schedule ``constants.MAX_DECISIONS``
    # decisions and a timer to force the scheduling of the remaining tasks.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    assert len(decisions) == constants.MAX_DECISIONS
    assert decisions[-1].type == "StartTimer"

    decision_id = history.last_id
    for i in range(constants.MAX_DECISIONS):
        history.add_activity_task(
            increment,
            decision_id=decision_id,
            activity_id=f"activity-tests.data.activities.increment-{i + 1}",
            last_state="completed",
            result=i + 1,
        )
    (history.add_decision_task_scheduled().add_decision_task_started())

    # Once the first batch of ``constants.MAX_DECISIONS`` tasks is finished,
    # the executor should schedule the 5 remaining ones.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    assert len(decisions) == 5

    for i in range(constants.MAX_DECISIONS - 1, constants.MAX_DECISIONS + 5):
        history.add_activity_task(
            increment,
            decision_id=decision_id,
            activity_id=f"activity-tests.data.activities.increment-{i + 1}",
            last_state="completed",
            result=i + 1,
        )
    (history.add_decision_task_scheduled().add_decision_task_started())

    # All tasks are finised, the executor should complete the workflow.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    workflow_completed = swf.models.decision.WorkflowExecutionDecision()
    workflow_completed.complete(result=None)

    assert decisions[0] == workflow_completed


class ATestDefinitionWithBigDecisionResponse(BaseTestWorkflow):
    """
    This workflow will schedule 2 enormous tasks so the response cannot be
    handled by SWF directly. NB: the constant is lowered to 82kB in test env,
    but 32kB are reserved for the context, so the true limit is 50kB, hence the
    test below (cannot schedule 2 * 30kB decisions).
    """

    def run(self):
        msg = "*" * 30000  # 30kB input at least
        results = []
        results.append(self.submit(print_message, msg))
        results.append(self.submit(print_message, msg))
        futures.wait(*results)


@mock_swf
def test_workflow_with_big_decision_response():
    workflow = ATestDefinitionWithBigDecisionResponse
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow)

    # The first time, the executor should schedule ``constants.MAX_DECISIONS``
    # decisions and a timer to force the scheduling of the remaining tasks.
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    assert len(decisions) == 2
    assert decisions[0].type == "ScheduleActivityTask"
    assert decisions[1].type == "StartTimer"


class OnFailureMixin:
    failed = False

    def on_failure(self, history, reason, details=None):
        self.failed = True


class ATestDefinitionFailWorkflow(OnFailureMixin, BaseTestWorkflow):
    """
    This workflow executes a single task that fails, then it explicitly fails
    the whole workflow.
    """

    def run(self):
        result = self.submit(raise_error)
        if result.exception:
            self.fail("error")

        return result.result


@mock_swf
@patch.object(Executor, "decref_workflow")
def test_workflow_failed_from_definition(mock_decref_workflow):
    workflow = ATestDefinitionFailWorkflow
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow)

    # Let's directly add the task in state ``failed`` to make the executor fail
    # the workflow.
    history.add_activity_task(
        raise_error,
        decision_id=history.last_id,
        activity_id="activity-tests.data.activities.raise_error-1",
        last_state="failed",
        result=None,
    )

    (history.add_decision_task_scheduled().add_decision_task_started())

    # Now the workflow definition calls ``Workflow.fail('error')`` that should
    # fail the whole workflow.
    decisions = executor.replay(Response(history=history, execution=None)).decisions

    assert executor.workflow.failed is True

    workflow_failed = swf.models.decision.WorkflowExecutionDecision()
    workflow_failed.fail(reason="Workflow execution failed: error")

    assert decisions[0] == workflow_failed


class ATestDefinitionActivityRaisesOnFailure(OnFailureMixin, BaseTestWorkflow):
    """
    This workflow executes a task that fails and has the ``raises_on_failure``
    flag set to ``True``. It means it will raise an exception in addition to
    filling the ``Future.exception``'s attribute.
    """

    def run(self):
        return self.submit(raise_on_failure).result


@mock_swf
@patch.object(Executor, "decref_workflow")
def test_workflow_activity_raises_on_failure(mock_decref_workflow):
    workflow = ATestDefinitionActivityRaisesOnFailure
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow)

    history.add_activity_task(
        raise_on_failure,
        decision_id=history.last_id,
        activity_id="activity-tests.data.activities.raise_on_failure-1",
        last_state="failed",
        reason="error",
    )

    (history.add_decision_task_scheduled().add_decision_task_started())

    # The executor should fail the workflow and extract the reason from the
    # exception raised in the workflow definition.
    decisions = executor.replay(Response(history=history, execution=None)).decisions

    assert executor.workflow.failed is True

    workflow_failed = swf.models.decision.WorkflowExecutionDecision()
    workflow_failed.fail(
        reason="Workflow execution error in " "activity-tests.data.activities.raise_on_failure: " '"error"',
        details=builder.DEFAULT_DETAILS,
    )

    assert decisions[0] == workflow_failed


class ATestOnFailureDefinition(OnFailureMixin, BaseTestWorkflow):
    def run(self):
        if self.submit(raise_error).exception:
            self.fail("FAIL")


@mock_swf
@patch.object(Executor, "decref_workflow")
def test_on_failure_callback(mock_decref_workflow):
    workflow = ATestOnFailureDefinition
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow)

    history.add_activity_task(
        raise_error,
        decision_id=history.last_id,
        activity_id="activity-tests.data.activities.raise_error-1",
        last_state="failed",
        reason="error",
    )

    (history.add_decision_task_scheduled().add_decision_task_started())

    # The executor should fail the workflow and extract the reason from the
    # exception raised in the workflow definition.
    decisions = executor.replay(Response(history=history, execution=None)).decisions

    assert executor.workflow.failed is True

    workflow_failed = swf.models.decision.WorkflowExecutionDecision()
    workflow_failed.fail(reason="Workflow execution failed: FAIL")

    assert decisions[0] == workflow_failed


class ATestMultipleScheduledActivitiesDefinition(BaseTestWorkflow):
    def run(self):
        a = self.submit(increment, 1)
        b = self.submit(increment, 2)
        c = self.submit(double, b)

        return [a.result, b.result, c.result]


@mock_swf
def test_multiple_scheduled_activities():
    """
    When ``Future.exception`` was made blocking if the future is not finished,
    :py:meth:`swf.executor.Executor.resume` did not check ``future.finished``
    before ``future.exception is None``. It mades the call to ``.resume()`` to
    block for the first scheduled task it encountered instead of returning it.
    This issue was fixed in commit 6398aa8.
    With the wrong behaviour, the call to ``executor.replay()`` would not
    schedule the ``double`` task even after the task represented by *b*
    (``self.submit(increment, 2)``) has completed.
    """
    workflow = ATestMultipleScheduledActivitiesDefinition
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow)

    decision_id = history.last_id
    (
        history.add_activity_task_scheduled(
            increment,
            decision_id=decision_id,
            activity_id="activity-tests.data.activities.increment-1",
            input={"args": 1},
        )
        # The right behaviour is to schedule the ``double`` task when *b* is in
        # state finished.
        .add_activity_task(
            increment,
            decision_id=decision_id,
            activity_id="activity-tests.data.activities.increment-2",
            last_state="completed",
            input={"args": 2},
            result="3",
        )
    )

    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], double)


@mock_swf
def test_activity_task_timeout():
    workflow = ATestDefinition
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow)
    decision_id = history.last_id
    (
        history.add_activity_task(
            increment,
            activity_id="activity-tests.data.activities.increment-1",
            decision_id=decision_id,
            last_state="timed_out",
            timeout_type="START_TO_CLOSE",
        )
    )

    decisions = executor.replay(Response(history=history, execution=None)).decisions
    # The task timed out and there is no retry.
    assert len(decisions) == 1
    check_task_scheduled_decision(decisions[0], double)


@mock_swf
def test_activity_task_timeout_retry():
    workflow = ATestDefinitionRetryActivity
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow)
    decision_id = history.last_id
    (
        history.add_activity_task(
            increment_retry,
            activity_id="activity-tests.data.activities.increment_retry-1",
            decision_id=decision_id,
            last_state="timed_out",
            timeout_type="START_TO_CLOSE",
        )
    )

    decisions = executor.replay(Response(history=history, execution=None)).decisions
    assert len(decisions) == 1
    check_task_scheduled_decision(decisions[0], increment_retry)


@mock_swf
def test_activity_task_timeout_raises():
    workflow = ATestDefinitionActivityRaisesOnFailure
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow)
    decision_id = history.last_id
    (
        history.add_activity_task(
            raise_on_failure,
            activity_id="activity-tests.data.activities.raise_on_failure-1",
            decision_id=decision_id,
            last_state="timed_out",
            timeout_type="START_TO_CLOSE",
        )
    )

    decisions = executor.replay(Response(history=history, execution=None)).decisions
    workflow_failed = swf.models.decision.WorkflowExecutionDecision()
    workflow_failed.fail(
        reason="Workflow execution error in "
        "activity-tests.data.activities.raise_on_failure: "
        '"TimeoutError(START_TO_CLOSE)"'
    )

    assert decisions[0] == workflow_failed


@mock_swf
def test_activity_not_found_schedule_failed():
    conn = boto.connect_swf()
    conn.register_domain("TestDomain", "50")

    workflow = ATestDefinition
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow)
    decision_id = history.last_id
    (
        history.add_activity_task_schedule_failed(
            activity_id="activity-tests.data.activities.increment-1",
            decision_id=decision_id,
            activity_type={"name": increment.name, "version": increment.version},
            cause="ACTIVITY_TYPE_DOES_NOT_EXIST",
        )
    )

    decisions = executor.replay(Response(history=history, execution=None)).decisions
    check_task_scheduled_decision(decisions[0], increment)


def raise_already_exists(activity):
    @functools.wraps(raise_already_exists)
    def wrapped(*args):
        raise swf.exceptions.AlreadyExistsError(
            "<ActivityType domain={} name={} version={} status=REGISTERED> "
            "already exists".format(DOMAIN.name, activity.name, activity.version)
        )

    return wrapped


@mock_swf
def test_activity_not_found_schedule_failed_already_exists():
    workflow = ATestDefinition
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow)
    decision_id = history.last_id
    (
        history.add_activity_task_schedule_failed(
            activity_id="activity-tests.data.activities.increment-1",
            decision_id=decision_id,
            activity_type={"name": increment.name, "version": increment.version},
            cause="ACTIVITY_TYPE_DOES_NOT_EXIST",
        )
    )

    with patch("swf.models.ActivityType.save", raise_already_exists(increment)):
        decisions = executor.replay(Response(history=history, execution=None)).decisions

    check_task_scheduled_decision(decisions[0], increment)


class ATestDefinitionMoreThanMaxOpenActivities(BaseTestWorkflow):
    """
    This workflow executes more tasks than the maximum number of decisions a
    decider can take once.
    """

    def run(self):
        results = self.map(increment, list(range(constants.MAX_OPEN_ACTIVITY_COUNT + 5)))
        futures.wait(*results)


@mock_swf
def test_more_than_1000_open_activities_scheduled():
    workflow = ATestDefinitionMoreThanMaxOpenActivities
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow)

    # The first time, the executor should schedule
    # ``constants.MAX_OPEN_ACTIVITY_COUNT`` decisions.
    # No timer because we wait for at least an activity to complete.
    for i in range(constants.MAX_OPEN_ACTIVITY_COUNT // constants.MAX_DECISIONS):
        decisions = executor.replay(Response(history=history, execution=None)).decisions
        assert len(decisions) == constants.MAX_DECISIONS

    decision_id = history.last_id
    for i in range(constants.MAX_OPEN_ACTIVITY_COUNT):
        history.add_activity_task(
            increment,
            decision_id=decision_id,
            activity_id=f"activity-tests.data.activities.increment-{i + 1}",
            last_state="scheduled",
            result=i + 1,
        )
    (history.add_decision_task_scheduled().add_decision_task_started())

    decisions = executor.replay(Response(history=history, execution=None)).decisions
    assert executor._open_activity_count == constants.MAX_OPEN_ACTIVITY_COUNT
    assert len(decisions) == 0


@mock_swf
def test_more_than_1000_open_activities_scheduled_and_running():
    def get_random_state():
        import random

        return random.choice(["scheduled", "started"])

    workflow = ATestDefinitionMoreThanMaxOpenActivities
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow)

    # The first time, the executor should schedule
    # ``constants.MAX_OPEN_ACTIVITY_COUNT`` decisions.
    # No timer because we wait for at least an activity to complete.
    for i in range(constants.MAX_OPEN_ACTIVITY_COUNT // constants.MAX_DECISIONS):
        decisions = executor.replay(Response(history=history, execution=None)).decisions
        assert len(decisions) == constants.MAX_DECISIONS

    decision_id = history.last_id
    for i in range(constants.MAX_OPEN_ACTIVITY_COUNT):
        history.add_activity_task(
            increment,
            decision_id=decision_id,
            activity_id=f"activity-tests.data.activities.increment-{i + 1}",
            last_state=get_random_state(),
            result=i + 1,
        )
    (history.add_decision_task_scheduled().add_decision_task_started())

    decisions = executor.replay(Response(history=history, execution=None)).decisions
    assert len(decisions) == 0


@mock_swf
def test_more_than_1000_open_activities_partial_max():
    workflow = ATestDefinitionMoreThanMaxOpenActivities
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow)
    decisions = executor.replay(Response(history=history, execution=None)).decisions

    first_decision_id = history.last_id
    for i in range(constants.MAX_OPEN_ACTIVITY_COUNT - 2):
        history.add_activity_task(
            increment,
            decision_id=first_decision_id,
            activity_id=f"activity-tests.data.activities.increment-{i + 1}",
            last_state="scheduled",
            result=i + 1,
        )
    (history.add_decision_task_scheduled().add_decision_task_started())

    decisions = executor.replay(Response(history=history, execution=None)).decisions
    assert executor._open_activity_count == constants.MAX_OPEN_ACTIVITY_COUNT
    assert len(decisions) == 2

    history.add_decision_task_completed()
    for i in range(2):
        id_ = constants.MAX_OPEN_ACTIVITY_COUNT - 2 + i + 1
        history.add_activity_task(
            increment,
            decision_id=history.last_id,
            activity_id=f"activity-tests.data.activities.increment-{id_}",
            last_state="scheduled",
            result=id_,
        )

    (history.add_decision_task_scheduled().add_decision_task_started())

    decisions = executor.replay(Response(history=history, execution=None)).decisions
    assert executor._open_activity_count == constants.MAX_OPEN_ACTIVITY_COUNT
    assert len(decisions) == 0

    history.add_decision_task_completed()

    for i in range(constants.MAX_OPEN_ACTIVITY_COUNT - 2):
        scheduled_id = first_decision_id + i + 1
        history.add_activity_task_started(scheduled_id)
        history.add_activity_task_completed(
            scheduled_id,
            started=history.last_id,
        )

    (history.add_decision_task_scheduled().add_decision_task_started())

    decisions = executor.replay(Response(history=history, execution=None)).decisions
    # 2 already scheduled + 5 to schedule now
    assert executor._open_activity_count == 7
    assert len(decisions) == 5


class ATestTaskNaming(BaseTestWorkflow):
    """
    This workflow executes a few tasks and tests the naming (task ID
    assignation) depending on their idempotence.
    """

    def run(self):
        results = []
        results.append(self.submit(increment, 0))
        results.append(self.submit(increment, 0))
        results.append(self.submit(triple, 1))
        results.append(self.submit(triple, 2))
        results.append(self.submit(triple, 2))
        results.append(self.submit(Tetra, 1))
        futures.wait(*results)


@mock_swf
def test_task_naming():
    workflow = ATestTaskNaming
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow, input={})

    decisions = executor.replay(Response(history=history, execution=None)).decisions
    expected = [
        # non idempotent task, should increment
        "activity-tests.data.activities.increment-1",
        # non idempotent task, should increment again
        "activity-tests.data.activities.increment-2",
        # idempotent task, with arg 1
        "activity-tests.data.activities.triple-bdc09455c37471e0ba7397350413a5e6",
        # idempotent task, with arg 2
        "activity-tests.data.activities.triple-12036b25db61ae6cadf7a003ff523029",
        # idempotent, not rescheduled
        # # idempotent task, with arg 2 too => same task id
        # "activity-tests.data.activities.triple-12036b25db61ae6cadf7a003ff523029",
        # class-based task, non idempotent
        "activity-tests.data.activities.Tetra-1",
    ]
    for i in range(0, len(expected)):
        decision = decisions[i]["scheduleActivityTaskDecisionAttributes"]
        assert decision["activityId"] == expected[i]


@mock_swf
def test_run_context():
    workflow = ATestTaskNaming
    executor = Executor(DOMAIN, workflow)

    history = builder.History(workflow, input={})

    executor.replay(
        Response(
            history=history,
            execution=swf.models.workflow.WorkflowExecution(
                domain=DOMAIN,
                workflow_id="a_workflow_id",
                run_id="a_run_id",
                workflow_type=swf.models.workflow.WorkflowType(
                    domain=DOMAIN,
                    name="the_workflow_name",
                    version="the_workflow_version",
                ),
            ),
        )
    )
    context = executor.get_run_context()
    expected = dict(
        name="the_workflow_name",
        version="the_workflow_version",
        domain_name=DOMAIN.name,
        workflow_id="a_workflow_id",
        run_id="a_run_id",
        tag_list=[],
        continued_execution_run_id=None,
        parent_workflow_id=None,
        parent_run_id=None,
    )
    assert expected == context


class ATestDefinitionChildWithIdWorkflow(BaseTestWorkflow):
    name = "test_child_workflow"

    @classmethod
    def get_workflow_id(cls, *args, **kwargs):
        return kwargs.get("workflow_name", None)

    def run(self, *args, **kwargs):
        return 42


class ATestDefinitionParentWorkflow(BaseTestWorkflow):
    name = "test_parent_workflow"

    def run(self):
        future = self.submit(ATestDefinitionChildWithIdWorkflow, workflow_name="workflow-child-one-1")
        futures.wait(future)


@mock_swf
def test_workflow_task_naming():
    workflow = ATestDefinitionParentWorkflow
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow, input={})
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    assert decisions == [
        {
            "decisionType": "StartChildWorkflowExecution",
            "startChildWorkflowExecutionDecisionAttributes": {
                "taskList": {"name": "test_task_list"},
                "workflowId": "workflow-child-one-1",
                "taskStartToCloseTimeout": "300",
                "executionStartToCloseTimeout": "3600",
                "workflowType": {
                    "name": "tests.test_simpleflow.test_dataflow.ATestDefinitionChildWithIdWorkflow",
                    "version": "test_version",
                },
                "input": json_dumps(
                    {
                        "args": [],
                        "kwargs": {"workflow_name": "workflow-child-one-1"},
                    }
                ),
            },
        }
    ]


class ATestDefinitionIdempotentChildWithIdWorkflow(BaseTestWorkflow):
    name = "test_child_workflow"
    idempotent = True

    def run(self, *args, **kwargs):
        return 42


class ATestDefinitionIdempotentParentWorkflow(BaseTestWorkflow):
    name = "test_parent_workflow"

    def run(self):
        future = self.submit(ATestDefinitionIdempotentChildWithIdWorkflow, a=1)
        futures.wait(future)


@mock_swf
def test_workflow_idempotent_task_naming():
    workflow = ATestDefinitionIdempotentParentWorkflow
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow, input={})
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    assert decisions == [
        {
            "decisionType": "StartChildWorkflowExecution",
            "startChildWorkflowExecutionDecisionAttributes": {
                "taskList": {"name": "test_task_list"},
                "workflowId": "workflow-test_child_workflow-None--0--adb86b0326491007eae44a0a692bfc53",
                "taskStartToCloseTimeout": "300",
                "executionStartToCloseTimeout": "3600",
                "workflowType": {
                    "name": "tests.test_simpleflow.test_dataflow.ATestDefinitionIdempotentChildWithIdWorkflow",
                    "version": "test_version",
                },
                "input": json_dumps(
                    {
                        "args": [],
                        "kwargs": {"a": 1},
                    }
                ),
            },
        }
    ]


class ATestDefinitionWithMarkersWorkflow(BaseTestWorkflow):
    name = "test_markers"

    def run(self):
        m1 = self.submit(self.record_marker("First marker"))
        m2 = self.submit(self.record_marker("First marker", "again"))
        self.second_marker_details = {
            "what": "Details for second marker",
            "date": datetime.date(2018, 1, 1),
        }
        m3 = self.submit(self.record_marker("Second marker", details=self.second_marker_details))
        futures.wait(m1, m2, m3)


@mock_swf
def test_markers():
    workflow = ATestDefinitionWithMarkersWorkflow
    executor = Executor(DOMAIN, workflow)
    history = builder.History(workflow, input={})
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    expected = [
        {
            "decisionType": "RecordMarker",
            "recordMarkerDecisionAttributes": {"markerName": "First marker"},
        },
        {
            "decisionType": "RecordMarker",
            "recordMarkerDecisionAttributes": {
                "details": '"again"',
                "markerName": "First marker",
            },
        },
        {
            "decisionType": "RecordMarker",
            "recordMarkerDecisionAttributes": {
                "details": json_dumps({"what": "Details for second marker", "date": "2018-01-01"}),
                "markerName": "Second marker",
            },
        },
        {
            "decisionType": "StartTimer",
            "startTimerDecisionAttributes": {
                "startToFireTimeout": "0",
                "timerId": "_simpleflow_wake_up_timer",
            },
        },
    ]
    assert expected == decisions


class ATestDefinitionNonPythonicWorkflow(BaseTestWorkflow):
    def run(self, *args, **kwargs):
        task = NonPythonicActivityTask(non_pythonic, *args, **kwargs)
        future = self.submit(task)
        futures.wait(future)


@mock_swf
def test_non_pythonic_activity_with_dict():
    workflow = ATestDefinitionNonPythonicWorkflow
    executor = Executor(DOMAIN, workflow)
    args = {
        "first_arg": 1,
        "second_arg": {"foo": "bar"},
    }
    history = builder.History(workflow, input={"kwargs": args})
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    expected = [
        {
            "decisionType": "ScheduleActivityTask",
            "scheduleActivityTaskDecisionAttributes": {
                "activityId": "activity-tests.data.activities.non_pythonic-1",
                "activityType": {
                    "name": "tests.data.activities.non_pythonic",
                    "version": "test",
                },
                "heartbeatTimeout": "300",
                "input": json_dumps(args),
                "scheduleToCloseTimeout": "300",
                "scheduleToStartTimeout": "300",
                "startToCloseTimeout": "300",
                "taskList": {"name": "default"},
            },
        }
    ]
    assert expected == decisions


@mock_swf
def test_non_pythonic_activity_with_array():
    workflow = ATestDefinitionNonPythonicWorkflow
    executor = Executor(DOMAIN, workflow)
    args = [
        1,
        {"foo": "bar"},
    ]
    history = builder.History(workflow, input={"args": args})
    decisions = executor.replay(Response(history=history, execution=None)).decisions
    expected = [
        {
            "decisionType": "ScheduleActivityTask",
            "scheduleActivityTaskDecisionAttributes": {
                "activityId": "activity-tests.data.activities.non_pythonic-1",
                "activityType": {
                    "name": "tests.data.activities.non_pythonic",
                    "version": "test",
                },
                "heartbeatTimeout": "300",
                "input": json_dumps(args),
                "scheduleToCloseTimeout": "300",
                "scheduleToStartTimeout": "300",
                "startToCloseTimeout": "300",
                "taskList": {"name": "default"},
            },
        }
    ]
    assert expected == decisions
