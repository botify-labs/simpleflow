from __future__ import annotations

import json
import unittest

import boto

from simpleflow import metrology, settings, storage
from simpleflow.activity import with_attributes
from simpleflow.constants import HOUR, MINUTE
from simpleflow.local.executor import Executor
from tests.moto_compat import mock_s3


@with_attributes(task_list="test_task_list")
class MyMetrologyTask(metrology.MetrologyTask):
    def __init__(self, num):
        self.num = num

    def execute(self):
        with self.step("Step1") as step:
            step.metadata["num"] = self.num
            step.read.records = self.num
        self.meta = "foo bar"


class MyWorkflow(metrology.MetrologyWorkflow):
    name = "test_workflow"
    version = "test_version"
    task_list = "test_task_list"
    decision_tasks_timeout = 5 * MINUTE
    execution_timeout = 1 * HOUR

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
        res = json.loads(storage.pull_content(settings.METROLOGY_BUCKET, "local/local/activity.0.json"))
        self.assertEqual(res["meta"], "foo bar")
        steps = res["steps"]
        self.assertEqual(steps[0]["name"], "Step1")
        self.assertEqual(steps[0]["read"]["records"], 1)
        self.assertEqual(steps[0]["metadata"]["num"], 1)

        res = json.loads(storage.pull_content(settings.METROLOGY_BUCKET, "local/local/metrology.json"))
        self.assertEqual(res[0][1]["metrology"]["meta"], "foo bar")
        self.assertEqual(res[0][1]["metrology"]["steps"][0]["name"], "Step1")
        self.assertEqual(res[0][1]["metrology"]["steps"][0]["read"]["records"], 1)
        self.assertEqual(res[0][1]["metrology"]["steps"][0]["metadata"]["num"], 1)


if __name__ == "__main__":
    unittest.main()
