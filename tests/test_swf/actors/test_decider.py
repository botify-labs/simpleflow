from __future__ import annotations

import unittest

import boto

from swf.actors import Decider
from swf.exceptions import PollTimeout
from swf.models import Domain
from tests.moto_compat import mock_swf


class TestActor(unittest.TestCase):
    def make_swf_environment(self):
        conn = boto.connect_swf()
        conn.register_domain("TestDomain", "50")
        conn.register_workflow_type(
            "TestDomain",
            "test-workflow",
            "v1.2",
            task_list="test-task-list",
            default_child_policy="TERMINATE",
            default_execution_start_to_close_timeout="6",
            default_task_start_to_close_timeout="3",
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
        conn.start_workflow_execution("TestDomain", "wfe-1234", "test-workflow", "v1.2")

        response = self.actor.poll()

        self.assertIsNotNone(response.token)
        self.assertEqual(
            [evt.type for evt in response.history],
            ["WorkflowExecution", "DecisionTask", "DecisionTask"],
        )
        self.assertEqual(response.execution.workflow_id, "wfe-1234")
        self.assertIsNotNone(response.execution.run_id)
