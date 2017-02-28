from __future__ import absolute_import

import json
import unittest

from moto import mock_swf, mock_s3
from simpleflow.activity import with_attributes
from simpleflow import workflow, task, storage, settings
from simpleflow.step import Step, WorkflowStepMixin
from simpleflow.swf.executor import Executor
from swf.models import Domain

import boto

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

    def run(self, num):
        self.force_steps = []
        activity_params = {
            "task_list": "steps_task_list"
        }
        self.prepare_step_config(
            s3_uri_prefix="s3://{}/data/".format(BUCKET),
            activity_params=activity_params
        )
        self.submit(
            Step('my_step', task.ActivityTask(MyTask, num)))


class StepTestCase(unittest.TestCase):

    def create_bucket(self):
        self.conn = boto.connect_s3()
        self.conn.create_bucket(BUCKET)

    @mock_s3
    @mock_swf
    def test_first_run(self):
        self.create_bucket()

        domain = Domain("TestDomain")
        executor = Executor(domain, MyWorkflow)
        workflow = MyWorkflow(executor)
        workflow.run(2)
