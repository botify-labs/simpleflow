import unittest

import boto.swf
from moto.swf import swf_backend


class MockSWFTestCase(unittest.TestCase):
    def setUp(self):
        self.conn = boto.connect_swf()
        self.conn.register_domain("TestDomain", "50")
        self.conn.register_workflow_type(
            "TestDomain", "test-workflow", "v1.2",
            task_list="test-task-list", default_child_policy="TERMINATE",
            default_execution_start_to_close_timeout="6",
            default_task_start_to_close_timeout="3",
        )

    def tearDown(self):
        swf_backend.reset()
        assert not self.conn.list_domains("REGISTERED")["domainInfos"], \
            "moto state incorrectly reset!"
