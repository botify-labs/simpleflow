import json
import unittest

from simpleflow.activity import with_attributes
from simpleflow import metrology, storage, settings
from simpleflow.local.executor import Executor

import boto
from moto import mock_s3


@with_attributes(task_list='test_task_list')
class MyMetrologyTask(metrology.MetrologyTask):

    def __init__(self, num):
        self.num = num

    def execute(self):
        with self.step('Step1') as step:
            step.metadata["num"] = self.num
            step.read.records = self.num


class MyWorkflow(metrology.MetrologyWorkflow):
    name = 'test_workflow'
    version = 'test_version'
    task_list = 'test_task_list'
    decision_tasks_timeout = '300'
    execution_timeout = '3600'
    tag_list = None  # FIXME should be optional
    child_policy = None  # FIXME should be optional

    def run(self, num):
        self.submit(MyMetrologyTask, num)


class MetrologyTestCase(unittest.TestCase):

    def create_bucket(self):
        self.conn = boto.connect_s3()
        self.conn.create_bucket(settings.METROLOGY_BUCKET)

    @mock_s3
    def test_metrology(self):
        self.create_bucket()
        ex = Executor(MyWorkflow)
        ex.run(input={"args": [1], "kwargs": {}})
        res = json.loads(storage.pull_content(
            settings.METROLOGY_BUCKET,
            "local/local/activity.0.json"))
        self.assertEquals(res[0]["name"], "Step1")
        self.assertEquals(res[0]["read"]["records"], 1)
        self.assertEquals(res[0]["metadata"]["num"], 1)

        res = json.loads(storage.pull_content(
            settings.METROLOGY_BUCKET,
            "local/local/metrology.json"))
        self.assertEquals(res[0][1]["metrology"][0]["name"], "Step1")
        self.assertEquals(res[0][1]["metrology"][0]["read"]["records"], 1)
        self.assertEquals(res[0][1]["metrology"][0]["metadata"]["num"], 1)
