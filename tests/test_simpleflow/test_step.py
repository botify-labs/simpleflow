from __future__ import absolute_import

import json
import unittest

from moto import mock_swf, mock_s3
from simpleflow.activity import with_attributes
from simpleflow import workflow, task, storage, step, futures
from simpleflow.step import Step, WorkflowStepMixin, should_force_step
from simpleflow.swf.executor import Executor
from swf.models import Domain
from swf.models.history import builder
from swf.responses import Response

import boto

BUCKET = "perfect_day"


def check_task_scheduled_decision(decision, task):
    """
    Asserts that *decision* schedules *task*.
    """
    assert decision['decisionType'] == 'ScheduleActivityTask'

    attributes = decision['scheduleActivityTaskDecisionAttributes']
    assert attributes['activityType']['name'] == task.name


def add_activity_task_from_decision(history, decision, activity, result=None, last_state="completed"):
    attributes = decision['scheduleActivityTaskDecisionAttributes']
    decision_id = history.last_id
    activity_id = attributes["activityId"]
    activity_input = attributes["input"]
    (history
        .add_activity_task(
        task.Activity(step.GetStepsDoneTask),
        decision_id=decision_id,
        activity_id=activity_id,
        last_state='completed',
        input=activity_input,
        result=result))


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

    def run(self, num, force_steps=[]):
        activity_params = {
            "task_list": "steps_task_list"
        }
        self.prepare_step_config(
            s3_bucket=BUCKET,
            s3_path_prefix="data/",
            activity_params=activity_params,
            force_steps=force_steps
        )
        taskf = self.submit(
            Step('my_step', task.ActivityTask(MyTask, num)))
        futures.wait(taskf)


class StepTestCase(unittest.TestCase):

    def create_bucket(self):
        self.conn = boto.connect_s3()
        self.conn.create_bucket(BUCKET)

    @mock_s3
    def test_get_steps_done(self):
        self.create_bucket()
        storage.push_content(BUCKET, "steps/mystep", "data")
        storage.push_content(BUCKET, "steps/mystep2", "data")
        t = step.GetStepsDoneTask(BUCKET, "steps/")
        res = t.execute()
        self.assertEquals(res, ["mystep", "mystep2"])

    @mock_s3
    def test_mark_step_done(self):
        self.create_bucket()
        t = step.MarkStepDoneTask(BUCKET, "steps/", "mystep")
        t.execute()
        self.assertEquals(
            storage.pull_content(BUCKET, "steps/mystep"),
            json.dumps(step.UNKNOWN_CONTEXT))

    @mock_s3
    @mock_swf
    def test_first_run(self):
        self.create_bucket()

        domain = Domain("TestDomain")
        executor = Executor(domain, MyWorkflow)
        history = builder.History(MyWorkflow, input={"args": [2]})
        decisions, _ = executor.replay(Response(history=history, execution=None))

        # Check that we call GetStepsDoneTask
        check_task_scheduled_decision(decisions[0], task.Activity(step.GetStepsDoneTask))

        # Now decide that it returns no step done
        add_activity_task_from_decision(history, decisions[0], task.Activity(step.GetStepsDoneTask), result=[])
        decisions, _ = executor.replay(Response(history=history, execution=None))

        # Check that we ask MyTask
        check_task_scheduled_decision(decisions[0], MyTask)

        # Execute the task and check the we call MarkStepDoneTask
        add_activity_task_from_decision(history, decisions[0], MyTask)
        decisions, _ = executor.replay(Response(history=history, execution=None))
        check_task_scheduled_decision(decisions[0], task.Activity(step.MarkStepDoneTask))

    @mock_s3
    @mock_swf
    def test_already_done(self):
        self.create_bucket()

        domain = Domain("TestDomain")
        executor = Executor(domain, MyWorkflow)
        history = builder.History(MyWorkflow, input={"args": [2]})
        decisions, _ = executor.replay(Response(history=history, execution=None))

        # Check that we call GetStepsDoneTask
        check_task_scheduled_decision(decisions[0], task.Activity(step.GetStepsDoneTask))

        # Now decide that it returns 'my_step' as done
        add_activity_task_from_decision(history, decisions[0], task.Activity(step.GetStepsDoneTask), result=['my_step'])
        decisions, _ = executor.replay(Response(history=history, execution=None))

        # Check that the workflow is done
        self.assertEquals(decisions[0]["decisionType"], "CompleteWorkflowExecution")

    @mock_s3
    @mock_swf
    def test_force_step(self):
        self.create_bucket()

        domain = Domain("TestDomain")
        executor = Executor(domain, MyWorkflow)
        # We force `my_step`
        history = builder.History(MyWorkflow, input={"args": [2], "kwargs": {"force_steps": ["my_step"]}})
        decisions, _ = executor.replay(Response(history=history, execution=None))

        # Check that we call GetStepsDoneTask
        check_task_scheduled_decision(decisions[0], task.Activity(step.GetStepsDoneTask))

        # Now decide that it returns 'my_step' as done
        add_activity_task_from_decision(history, decisions[0], task.Activity(step.GetStepsDoneTask), result=['my_step'])
        decisions, _ = executor.replay(Response(history=history, execution=None))

        # Check that we ask MyTask even if my_step was returned as done
        check_task_scheduled_decision(decisions[0], MyTask)

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
