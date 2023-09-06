from __future__ import annotations

import unittest

import boto3
from moto import mock_swf

from simpleflow.swf.mapper.actors import Decider
from simpleflow.swf.mapper.exceptions import PollTimeout
from simpleflow.swf.mapper.models.domain import Domain


class TestActor(unittest.TestCase):
    def make_swf_environment(self):
        conn = boto3.client("swf", region_name="us-east-1")
        conn.register_domain(name="TestDomain", workflowExecutionRetentionPeriodInDays="50")
        conn.register_workflow_type(
            domain="TestDomain",
            name="test-workflow",
            version="v1.2",
            defaultTaskList={"name": "test-task-list"},
            defaultChildPolicy="TERMINATE",
            defaultExecutionStartToCloseTimeout="10",
            defaultTaskStartToCloseTimeout="3",
        )
        return conn

    def setUp(self):
        self.domain = Domain("TestDomain")
        self.actor = Decider(self.domain, "test-task-list")

    def tearDown(self):
        pass

    @mock_swf
    def test_poll_with_no_decision_to_take(self):
        _ = self.make_swf_environment()
        with self.assertRaises(PollTimeout):
            self.actor.poll()

    @mock_swf
    def test_poll_with_decision_to_take(self):
        conn = self.make_swf_environment()
        conn.start_workflow_execution(
            domain="TestDomain",
            workflowId="wfe-1234",
            workflowType={"name": "test-workflow", "version": "v1.2"},
        )

        response = self.actor.poll()

        self.assertIsNotNone(response.token)
        self.assertEqual(
            [evt.type for evt in response.history],
            ["WorkflowExecution", "DecisionTask", "DecisionTask"],
        )
        self.assertEqual(response.execution.workflow_id, "wfe-1234")
        self.assertIsNotNone(response.execution.run_id)
