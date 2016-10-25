import os
import re
import time
import unittest

from psutil import Process
from setproctitle import setproctitle

from simpleflow.process import Supervisor


# Dummy function used in worker tests
def sleep_long(seconds):
    setproctitle("simpleflow Worker(sleep_long, {})".format(seconds))
    time.sleep(seconds)


class TestSupervisor(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        # cleanup all child processes
        if os.getenv("SIMPLEFLOW_CLEANUP_PROCESSES"):
            process = Process()
            for child in process.children(recursive=True):
                # TODO: have a warning here? normal?
                child.kill()

    def assertProcess(self, regex, count=1):
        children = Process().children(recursive=True)
        matching = [p for p in children if re.search(regex, p.name())]
        self.assertEqual(
            len(matching), count,
            "Expected {} processes matching {}, found {} in {}.".format(
                count, regex, len(matching), children
            )
        )

    def test_start(self):
        supervisor = Supervisor(sleep_long, arguments=(30,), nb_children=2)
        supervisor.start()
        # we need to wait a little here so the process starts and gets its name set
        # TODO: find a non-sleep approach to this
        time.sleep(0.5)
        self.assertProcess(r'simpleflow Supervisor\(nb_children=2\)')
        self.assertProcess(r'simpleflow Worker\(sleep_long, 30\)', count=2)
