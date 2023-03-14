from __future__ import annotations

import unittest

import boto
import boto.swf
from moto.swf import swf_backend

from simpleflow.swf.executor import Executor
from simpleflow.swf.process.worker.base import ActivityPoller, ActivityWorker
from swf.actors import Decider
from tests.data.constants import DOMAIN
from tests.moto_compat import mock_s3, mock_swf


@mock_s3
@mock_swf
class MockSWFTestCase(unittest.TestCase):
    def setUp(self):
        # SWF preparation
        self.domain = DOMAIN
        self.workflow_type_name = "test-workflow"
        self.workflow_type_version = "v1.2"
        self.decision_task_list = "test-task-list"

        self.conn = boto.connect_swf()
        self.conn.register_domain(self.domain.name, "50")
        self.conn.register_workflow_type(
            self.domain.name,
            self.workflow_type_name,
            self.workflow_type_version,
            task_list=self.decision_task_list,
            default_child_policy="TERMINATE",
            default_execution_start_to_close_timeout="6",
            default_task_start_to_close_timeout="3",
        )

        # S3 preparation in case we use jumbo fields
        self.s3_conn = boto.connect_s3()
        self.s3_conn.create_bucket("jumbo-bucket")

    def tearDown(self):
        swf_backend.reset()
        assert not self.conn.list_domains("REGISTERED")["domainInfos"], "moto state incorrectly reset!"

    def register_activity_type(self, func, task_list):
        self.conn.register_activity_type(self.domain.name, func, task_list)

    def start_workflow_execution(self, input=None):
        self.workflow_id = "wfe-1234"
        response = self.conn.start_workflow_execution(
            self.domain.name,
            self.workflow_id,
            self.workflow_type_name,
            self.workflow_type_version,
            input=input,
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
        return self.conn.get_workflow_execution_history(
            self.domain.name,
            workflow_id=self.workflow_id,
            run_id=self.run_id,
        )

    def process_activity_task(self):
        poller = ActivityPoller(self.domain, "default")
        response = poller.poll(identity="tst")
        worker = ActivityWorker()
        worker.process(poller, response.task_token, response.activity_task)
