from __future__ import annotations

import unittest

import boto3
from moto import mock_swf, mock_s3
from moto.swf import swf_backend

from simpleflow.swf.executor import Executor
from simpleflow.swf.process.worker.base import ActivityPoller, ActivityWorker
from simpleflow.swf.mapper.actors import Decider
from tests.data.constants import DOMAIN


@mock_s3
@mock_swf
class MockSWFTestCase(unittest.TestCase):
    def setUp(self):
        # SWF preparation
        self.domain = DOMAIN
        self.workflow_type_name = "test-workflow"
        self.workflow_type_version = "v1.2"
        self.decision_task_list = "test-task-list"

        self.swf_conn = boto3.client("swf", region_name="us-east-1")
        self.swf_conn.register_domain(name=self.domain.name, workflowExecutionRetentionPeriodInDays="50")
        self.swf_conn.register_workflow_type(
            domain=self.domain.name,
            name=self.workflow_type_name,
            version=self.workflow_type_version,
            defaultTaskList={"name": self.decision_task_list},
            defaultChildPolicy="TERMINATE",
            defaultExecutionStartToCloseTimeout="10",
            defaultTaskStartToCloseTimeout="3",
        )

        # S3 preparation in case we use jumbo fields
        self.s3_conn = boto3.client("s3", region_name="us-east-1")
        self.s3_conn.create_bucket(Bucket="jumbo-bucket")

    def tearDown(self):
        swf_backend.reset()
        assert not self.swf_conn.list_domains(registrationStatus="REGISTERED")[
            "domainInfos"
        ], "moto state incorrectly reset!"

    def register_activity_type(self, func: str, task_list: str):
        self.swf_conn.register_activity_type(domain=self.domain.name, name=func, version=task_list)

    def start_workflow_execution(self, input=None):
        self.workflow_id = "wfe-1234"
        response = self.swf_conn.start_workflow_execution(
            domain=self.domain.name,
            workflowId=self.workflow_id,
            workflowType={
                "name": self.workflow_type_name,
                "version": self.workflow_type_version,
            },
            input=input if input is not None else "",
            executionStartToCloseTimeout="10",
        )
        self.run_id = response["runId"]

    def build_decisions(self, workflow_class):
        self.decider = Decider(self.domain, self.decision_task_list)
        response = self.decider.poll()
        self._decision_token = response.token
        self.executor = Executor(self.domain, workflow_class)
        return self.executor.replay(response)

    def take_decisions(self, decisions, execution_context=None):
        self.decider.complete(
            self._decision_token,
            decisions=decisions,
            execution_context=execution_context,
        )

    def get_workflow_execution_history(self):
        return self.swf_conn.get_workflow_execution_history(
            domain=self.domain.name,
            execution={
                "workflowId": self.workflow_id,
                "runId": self.run_id,
            },
        )

    def process_activity_task(self):
        poller = ActivityPoller(self.domain, "default")
        response = poller.poll(identity="tst")
        worker = ActivityWorker()
        worker.process(poller, response.task_token, response.activity_task)
