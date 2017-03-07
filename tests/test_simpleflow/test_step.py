import json
import unittest

from mock import patch
from moto import mock_swf, mock_s3
import boto

from simpleflow.activity import with_attributes
from simpleflow import workflow, task, storage, step, futures
from simpleflow.step.submittable import Step
from simpleflow.step.workflow import WorkflowStepMixin
from simpleflow.step.tasks import GetStepsDoneTask, MarkStepDoneTask
from simpleflow.step.utils import (
    should_force_step,
    get_step_force_reasons
)
from simpleflow.step.constants import UNKNOWN_CONTEXT
from .base import TestWorkflowMixin


BUCKET = "perfect_day"


@with_attributes(task_list='test_task_list')
class MyTask(object):
    def __init__(self, num):
        self.num = num

    def execute(self):
        return self.num * 2


class MyWorkflow(workflow.Workflow, WorkflowStepMixin):
    name = 'test_workflow'
    version = 'test_version'
    task_list = 'test_task_list'
    decision_tasks_timeout = '300'
    execution_timeout = '3600'

    def get_activity_params(self):
        return {
            "task_list": "steps_task_list"
        }

    def run(self, num, force_steps=[]):
        self.add_forced_steps(force_steps, "workflow_init")

        taskf = self.submit(
            Step('my_step',
                 task.ActivityTask(MyTask, num),
                 force_steps_if_executed=['my_step_2']))
        futures.wait(taskf)


class StepTestCase(unittest.TestCase, TestWorkflowMixin):
    WORKFLOW = MyWorkflow

    def create_bucket(self):
        self.conn = boto.connect_s3()
        self.conn.create_bucket(BUCKET)

    @mock_s3
    def test_get_steps_done(self):
        self.create_bucket()
        storage.push_content(BUCKET, "steps/mystep", "data")
        storage.push_content(BUCKET, "steps/mystep2", "data")
        t = GetStepsDoneTask(BUCKET, "steps")
        res = t.execute()
        self.assertEquals(res, ["mystep", "mystep2"])

    @mock_s3
    def test_mark_step_done(self):
        self.create_bucket()
        t = MarkStepDoneTask(BUCKET, "steps/", "mystep")
        t.execute()
        self.assertEquals(
            storage.pull_content(BUCKET, "steps/mystep"),
            json.dumps(UNKNOWN_CONTEXT))

    @mock_s3
    @mock_swf
    def test_first_run(self):
        self.create_bucket()

        self.build_history({"args": [2]})
        decisions = self.replay()

        # Check that we call GetStepsDoneTask
        self.check_task_scheduled_decision(decisions[0], task.Activity(GetStepsDoneTask))

        # Now decide that it returns no step done
        self.add_activity_task_from_decision(decisions[0], task.Activity(GetStepsDoneTask), result=[])
        decisions = self.replay()

        # Check that we ask MyTask
        self.check_task_scheduled_decision(decisions[0], MyTask)

        # Execute the task and check the we call MarkStepDoneTask
        self.add_activity_task_from_decision(decisions[0], MyTask)
        decisions = self.replay()
        self.check_task_scheduled_decision(decisions[0], task.Activity(MarkStepDoneTask))

        # Check that we'll force the step 'my_step_3'
        self.assertEquals(self.executor._workflow.get_forced_steps(), ["my_step_2"])

    @mock_s3
    @mock_swf
    def test_already_done(self):
        self.create_bucket()

        self.build_history({"args": [2]})
        decisions = self.replay()

        # Check that we call GetStepsDoneTask
        self.check_task_scheduled_decision(decisions[0], task.Activity(GetStepsDoneTask))

        # Now decide that it returns 'my_step' as done
        self.add_activity_task_from_decision(decisions[0], task.Activity(GetStepsDoneTask), result=['my_step'])

        # Call Marker Step is done
        # Check that the workflow is done
        decisions = self.replay()
        self.assertEquals(decisions[0]["decisionType"], "RecordMarker")
        self.assertEquals(
            json.loads(decisions[0]["recordMarkerDecisionAttributes"]["details"]),
            {"status":"skipped", "forced": False, "step":"my_step", "reasons":[]})

    @mock_s3
    @mock_swf
    def test_force_step(self):
        self.create_bucket()

        self.build_history({"args": [2], "kwargs": {"force_steps": ["my_step"]}})
        decisions = self.replay()

        # Check that we call GetStepsDoneTask
        self.check_task_scheduled_decision(decisions[0], task.Activity(GetStepsDoneTask))

        # Now decide that it returns 'my_step' as done
        self.add_activity_task_from_decision(decisions[0], task.Activity(GetStepsDoneTask), result=['my_step'])
        decisions = self.replay()

        # Check that we ask MyTask even if my_step was returned as done
        self.check_task_scheduled_decision(decisions[0], MyTask)
        self.add_activity_task_from_decision(decisions[0], task.Activity(MyTask))

        decisions = self.replay()
        self.check_task_scheduled_decision(decisions[0], task.Activity(MarkStepDoneTask))
        self.add_activity_task_from_decision(decisions[0], task.Activity(MarkStepDoneTask))

        decisions = self.replay()
        self.assertEquals(decisions[0]["decisionType"], "RecordMarker")
        self.assertEquals(
            json.loads(decisions[0]["recordMarkerDecisionAttributes"]["details"]),
            {"status": "scheduled", "forced": True, "step":"my_step", "reasons":["workflow_init"]})

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
        reasons = {
            "a.b": ["MY_REASON"],
            "a": ["MY_ROOT_REASON"]
        }
        self.assertEquals(
            sorted(get_step_force_reasons(step_name, reasons)),
            ["MY_REASON", "MY_ROOT_REASON"])
