import re
import unittest

from psutil import Process

from simpleflow.process import Supervisor


class TestSupervisor(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        # cleanup all child processes
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

    def test_init(self):
        manager = Supervisor()
        manager.boot()
        self.assertProcess(r'python')
