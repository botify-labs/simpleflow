from __future__ import annotations

import os
import re
import signal
import unittest

from psutil import NoSuchProcess, Process


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
            len(matching),
            count,
            "Expected {} processes matching {}, found {} in {}.".format(count, regex, len(matching), children),
        )
