import os
import re
import signal
import unittest

import boto
from moto import mock_swf
from psutil import Process, NoSuchProcess

from tests.data import WORKFLOW, DEFAULT_VERSION, TASK_LIST

DOMAIN = "TestDomain"


class IntegrationTestCase(unittest.TestCase):
    def tearDown(self):
        # cleanup all child processes
        if os.getenv("SIMPLEFLOW_CLEANUP_PROCESSES"):
            process = Process()
            for child in process.children(recursive=True):
                # TODO: have a warning here? normal?
                try:
                    child.kill()
                except NoSuchProcess:
                    pass
        # reset SIGTERM handler
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    def assertProcess(self, regex, count=1):
        children = Process().children(recursive=True)
        matching = [p for p in children if re.search(regex, p.name())]
        self.assertEqual(
            len(matching), count,
            "Expected {} processes matching {}, found {} in {}.".format(
                count, regex, len(matching), children
            )
        )


@mock_swf
class SimpleflowTestCase(unittest.TestCase):
    def setUp(self):
        self.conn = boto.connect_swf()
        self.conn.register_domain(DOMAIN, "365")
        self.conn.register_workflow_type(
            DOMAIN,
            WORKFLOW,
            DEFAULT_VERSION,
            task_list=TASK_LIST,
            default_child_policy="TERMINATE",
            default_execution_start_to_close_timeout="6",
            default_task_start_to_close_timeout="3",
        )
        self.conn.start_workflow_execution(
            DOMAIN,
            "wfe-1234",
            WORKFLOW,
            DEFAULT_VERSION
        )
