from __future__ import annotations

import json
import unittest

import boto3
from moto import mock_s3, mock_swf

from simpleflow import futures, storage, task, workflow
from simpleflow.activity import with_attributes
from simpleflow.canvas import Chain
from simpleflow.constants import HOUR, MINUTE
from simpleflow.local import Executor
from simpleflow.step.constants import STEPS_DONE_MARKER_PREFIX, UNKNOWN_CONTEXT
from simpleflow.step.submittable import Step
from simpleflow.step.tasks import GetStepsDoneTask, MarkStepDoneTask
from simpleflow.step.utils import (
    get_step_force_reasons,
    should_force_step,
    step_will_run,
)
from simpleflow.step.workflow import WorkflowStepMixin

from .base import TestWorkflowMixin

BUCKET = "perfect_day"


@with_attributes(task_list="test_task_list")
class MyTask:
    def __init__(self, num):
        self.num = num

    def execute(self):
        return self.num * 2


class CustomExecutor(Executor):
    def __init__(self, workflow_class):
        super().__init__(workflow_class)
        self.create_workflow()

    def submit(self, func, *args, **kwargs):
        if hasattr(func, "activity") and func.activity == MyTask:
            f = futures.Future()
            f.set_running()
            return f
        return super().submit(func, *args, **kwargs)


class MyWorkflow(workflow.Workflow, WorkflowStepMixin):
    name = "test_workflow"
    version = "test_version"
    task_list = "test_task_list"
    decision_tasks_timeout = 5 * MINUTE
    execution_timeout = 1 * HOUR

    def get_activity_params(self):
        return {"task_list": "steps_task_list"}

    def run(self, num, force_steps=None, skip_steps=None):
        self.add_forced_steps(force_steps or [], "workflow_init")
        self.add_skipped_steps(skip_steps or [], "workflow_init")

        taskf = self.submit(
            Step(
                "my_step",
                task.ActivityTask(MyTask, num),
                force_steps_if_executed=["my_step_2"],
            )
        )
        futures.wait(taskf)

    def get_step_bucket(self):
        return BUCKET


class StepTestCase(unittest.TestCase, TestWorkflowMixin):
    WORKFLOW = MyWorkflow

    def create_bucket(self):
        self.client = boto3.client("s3", region_name="us-east-1")
        self.client.create_bucket(Bucket=BUCKET)

    @mock_s3
    def test_get_steps_done(self):
        self.create_bucket()
        storage.push_content(BUCKET, "steps/mystep", "data")
        storage.push_content(BUCKET, "steps/mystep2", "data")
        t = GetStepsDoneTask(BUCKET, "steps")
        res = t.execute()
        self.assertEqual(res, ["mystep", "mystep2"])

    @mock_s3
    def test_mark_step_done(self):
        self.create_bucket()
        t = MarkStepDoneTask(BUCKET, "steps/", "mystep")
        t.execute()
        self.assertEqual(storage.pull_content(BUCKET, "steps/mystep"), json.dumps(UNKNOWN_CONTEXT))

    @mock_s3
    @mock_swf
    def _test_first_run(self):
        """
        Commented while waiting for marker's swf mocks on moto
        """
        self.create_bucket()

        self.build_history({"args": [2]})
        decisions = self.replay()

        # Check that we call GetStepsDoneTask
        self.check_task_scheduled_decision(decisions[0], task.Activity(GetStepsDoneTask))

        # Now decide that it returns no step done
        self.add_activity_task_from_decision(decisions[0], task.Activity(GetStepsDoneTask), result=[])

        # Call marker
        decisions = self.replay()
        self.assertEqual(decisions[0]["decisionType"], "RecordMarker")
        self.assertEqual(
            json.loads(decisions[0]["recordMarkerDecisionAttributes"]["details"]),
            {
                "status": "scheduled",
                "forced": True,
                "step": "my_step",
                "reasons": ["workflow_init"],
            },
        )

        # Check that we ask MyTask
        decisions = self.replay()
        self.check_task_scheduled_decision(decisions[0], MyTask)

        # Execute the task and check the we call MarkStepDoneTask
        self.add_activity_task_from_decision(decisions[0], MyTask)
        decisions = self.replay()
        self.check_task_scheduled_decision(decisions[0], task.Activity(MarkStepDoneTask))

        # Check that we'll force the step 'my_step_3'
        self.assertEqual(self.executor._workflow.get_forced_steps(), ["my_step_2"])

    @mock_s3
    @mock_swf
    def test_already_done(self):
        self.create_bucket()

        self.build_history({"args": [2]})
        decisions = self.replay()

        # Check that we call GetStepsDoneTask
        self.check_task_scheduled_decision(decisions[0], task.Activity(GetStepsDoneTask))

        # Now decide that it returns 'my_step' as done
        self.add_activity_task_from_decision(decisions[0], task.Activity(GetStepsDoneTask), result=["my_step"])

        # Call Marker Step is done
        # Check that the workflow is done
        decisions = self.replay()
        self.assertEqual(decisions[0]["decisionType"], "RecordMarker")
        self.assertEqual(
            json.loads(decisions[0]["recordMarkerDecisionAttributes"]["details"]),
            {
                "status": "skipped",
                "forced": False,
                "step": "my_step",
                "reasons": ["Step was already played"],
            },
        )

    @mock_s3
    @mock_swf
    def _test_force_step(self):
        """
        Commented while waiting for marker's swf mocks on moto
        """
        self.create_bucket()

        self.build_history({"args": [2], "kwargs": {"force_steps": ["my_step"]}})
        decisions = self.replay()

        # Check that we call GetStepsDoneTask
        self.check_task_scheduled_decision(decisions[0], task.Activity(GetStepsDoneTask))

        # Now decide that it returns 'my_step' as done
        self.add_activity_task_from_decision(decisions[0], task.Activity(GetStepsDoneTask), result=["my_step"])
        decisions = self.replay()

        # Call marker
        self.assertEqual(decisions[0]["decisionType"], "RecordMarker")
        self.assertEqual(
            json.loads(decisions[0]["recordMarkerDecisionAttributes"]["details"]),
            {
                "status": "scheduled",
                "forced": True,
                "step": "my_step",
                "reasons": ["workflow_init"],
            },
        )

        # Check that we ask MyTask even if my_step was returned as done
        decisions = self.replay()
        self.check_task_scheduled_decision(decisions[0], MyTask)
        self.add_activity_task_from_decision(decisions[0], task.Activity(MyTask))

        decisions = self.replay()
        self.check_task_scheduled_decision(decisions[0], task.Activity(MarkStepDoneTask))
        self.add_activity_task_from_decision(decisions[0], task.Activity(MarkStepDoneTask))

        decisions = self.replay()
        self.assertEqual(decisions[0]["decisionType"], "RecordMarker")
        self.assertEqual(
            json.loads(decisions[0]["recordMarkerDecisionAttributes"]["details"]),
            {
                "status": "completed",
                "forced": False,
                "step": "my_step",
                "reasons": ["workflow_init"],
            },
        )

    @mock_s3
    @mock_swf
    def test_skip_step(self):
        """
        Commented while waiting for marker's swf mocks on moto
        """
        self.create_bucket()

        self.build_history({"args": [2], "kwargs": {"skip_steps": ["my_step"]}})
        decisions = self.replay()

        # Check that we call GetStepsDoneTask
        self.check_task_scheduled_decision(decisions[0], task.Activity(GetStepsDoneTask))

        # Now decide that it returns 'my_step' as done
        self.add_activity_task_from_decision(decisions[0], task.Activity(GetStepsDoneTask), result=[])
        decisions = self.replay()

        # Call marker
        self.assertEqual(decisions[0]["decisionType"], "RecordMarker")
        self.assertEqual(
            json.loads(decisions[0]["recordMarkerDecisionAttributes"]["details"]),
            {
                "status": "skipped",
                "forced": True,
                "step": "my_step",
                "reasons": ["workflow_init"],
            },
        )

    def test_should_force_step(self):
        step_name = "a.b.c"

        force_step = ["*"]
        self.assertTrue(should_force_step(step_name, force_step))
        force_step = "*"
        self.assertTrue(should_force_step(step_name, force_step))
        force_step = ["a"]
        self.assertTrue(should_force_step(step_name, force_step))
        force_step = ["a.b"]
        self.assertTrue(should_force_step(step_name, force_step))
        force_step = ["a.b.c"]
        self.assertTrue(should_force_step(step_name, force_step))

        force_step = ["a.c"]
        self.assertFalse(should_force_step(step_name, force_step))
        force_step = ["a.b.cd"]
        self.assertFalse(should_force_step(step_name, force_step))

    def test_reasons(self):
        step_name = "a.b.c"
        reasons = {"a.b": ["MY_REASON"], "a": ["MY_ROOT_REASON"]}
        self.assertEqual(
            sorted(get_step_force_reasons(step_name, reasons)),
            ["MY_REASON", "MY_ROOT_REASON"],
        )

    def test_step_will_run_skipped(self):
        self.assertFalse(step_will_run("a.b.c", [], ["a.b"], ["a.b"]))
        self.assertFalse(step_will_run("a.b.c", [], ["a.b"], []))
        self.assertTrue(step_will_run("a.b.c", [], ["b"], ["a.b"]))

    @mock_s3
    @mock_swf
    def test_propagate_attribute(self):
        """
        Test that attribute 'raises_on_failure' is well propagated through Step.
        """
        self.create_bucket()
        executor = CustomExecutor(MyWorkflow)
        executor.initialize_history({})

        activities = Chain(
            (MyTask, 1),
            (MyTask, 2),
        )
        step_act = Step("test_propagate_attribute", activities)
        Chain(step_act, raises_on_failure=False).submit(executor)

        self.assertFalse(activities.activities[0].activity.raises_on_failure)
        self.assertFalse(activities.activities[1].activity.raises_on_failure)


class MyDeciderWorkflow(MyWorkflow):
    steps_in_decider = True


class StepInDeciderTestCase(unittest.TestCase, TestWorkflowMixin):
    WORKFLOW = MyDeciderWorkflow

    def create_bucket(self):
        self.client = boto3.client("s3", region_name="us-east-1")
        self.client.create_bucket(Bucket=BUCKET)

    def record_markers(self, decisions):
        """
        Add the RecordMarker decisions to the history.
        """
        for decision in decisions:
            if decision["decisionType"] == "RecordMarker":
                attributes = decision["recordMarkerDecisionAttributes"]
                self.history.add_marker(attributes["markerName"], json.loads(attributes["details"]))

    def decision_types(self, decisions):
        return [d["decisionType"] for d in decisions]

    def marker_details(self, decision):
        assert decision["decisionType"] == "RecordMarker"
        return json.loads(decision["recordMarkerDecisionAttributes"]["details"])

    @mock_s3
    @mock_swf
    def test_first_run(self):
        self.create_bucket()

        self.build_history({"args": [2]})
        decisions = self.replay()

        # No GetStepsDoneTask: S3 is listed by the decider and the listing is memoized
        self.assertEqual(self.decision_types(decisions), ["RecordMarker", "RecordMarker", "StartTimer"])
        self.assertTrue(
            decisions[0]["recordMarkerDecisionAttributes"]["markerName"].startswith(STEPS_DONE_MARKER_PREFIX + ".")
        )
        self.assertEqual(
            self.marker_details(decisions[0]),
            {"bucket": BUCKET, "path": "default/steps/", "steps_done": []},
        )
        self.assertEqual(
            self.marker_details(decisions[1]),
            {"step": "my_step", "status": "scheduled", "forced": False, "reasons": []},
        )
        self.record_markers(decisions)

        # The step is now done on S3 (e.g. by another execution): replays must ignore it
        storage.push_content(BUCKET, "default/steps/my_step", "data")
        decisions = self.replay()
        self.check_task_scheduled_decision(decisions[0], MyTask)

        # Once the activities are done, the decider marks the step done: no MarkStepDoneTask
        self.add_activity_task_from_decision(decisions[0], task.Activity(MyTask), result=4)
        decisions = self.replay()
        self.assertEqual(self.decision_types(decisions), ["RecordMarker", "StartTimer"])
        self.assertEqual(
            self.marker_details(decisions[0]),
            {"step": "my_step", "status": "completed", "forced": False, "reasons": []},
        )
        self.assertEqual(storage.pull_content(BUCKET, "default/steps/my_step"), json.dumps(UNKNOWN_CONTEXT))
        self.assertEqual(self.executor._workflow.get_forced_steps(), ["my_step_2"])

        # The "completed" marker is recorded: the step isn't pushed again
        self.record_markers(decisions)
        self.client.delete_object(Bucket=BUCKET, Key="default/steps/my_step")
        decisions = self.replay()
        self.assertEqual(self.decision_types(decisions), ["CompleteWorkflowExecution"])
        self.assertEqual(storage.list_keys(BUCKET, "default/steps/"), [])

    @mock_s3
    @mock_swf
    def test_already_done(self):
        self.create_bucket()
        storage.push_content(BUCKET, "default/steps/my_step", "data")

        self.build_history({"args": [2]})
        decisions = self.replay()

        self.assertEqual(self.decision_types(decisions), ["RecordMarker", "RecordMarker", "StartTimer"])
        self.assertEqual(self.marker_details(decisions[0])["steps_done"], ["my_step"])
        self.assertEqual(
            self.marker_details(decisions[1]),
            {"step": "my_step", "status": "skipped", "forced": False, "reasons": ["Step was already played"]},
        )

        # The step was removed from S3: replays still use the memoized listing
        self.record_markers(decisions)
        self.client.delete_object(Bucket=BUCKET, Key="default/steps/my_step")
        decisions = self.replay()
        self.assertEqual(self.decision_types(decisions), ["CompleteWorkflowExecution"])

    @mock_s3
    def test_local_executor(self):
        self.create_bucket()

        executor = Executor(MyDeciderWorkflow)
        executor.run({"args": [2]})
        self.assertEqual(
            [m.details["status"] for m in executor.list_markers(all=True) if m.name == "log.step"],
            ["scheduled", "completed"],
        )
        self.assertEqual(GetStepsDoneTask(BUCKET, "local/steps/").execute(), ["my_step"])

        executor = Executor(MyDeciderWorkflow)
        executor.run({"args": [2]})
        self.assertEqual(
            [m.details["status"] for m in executor.list_markers(all=True) if m.name == "log.step"],
            ["skipped"],
        )
