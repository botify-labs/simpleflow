import multiprocessing
import os
import re
import signal
import time
import unittest

from psutil import Process, NoSuchProcess
from setproctitle import setproctitle

from simpleflow.process import Supervisor


class TestSupervisor(unittest.TestCase):
    def setUp(self):
        pass

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
        # dummy function used in following tests
        def sleep_long(seconds):
            setproctitle("simpleflow Worker(sleep_long, {})".format(seconds))
            time.sleep(seconds)

        # create a supervisor sub-process
        supervisor = Supervisor(sleep_long, arguments=(30,), nb_children=2)
        supervisor.start()

        # we need to wait a little here so the process starts and gets its name set
        # TODO: find a non-sleep approach to this
        time.sleep(0.5)
        self.assertProcess(r'simpleflow Supervisor\(nb_children=2\)')
        self.assertProcess(r'simpleflow Worker\(sleep_long, 30\)', count=2)

    def test_terminate(self):
        # custom function that handles sigterm by changing its name, so we can
        # test it effectively received a SIGTERM (maybe there's a better way?)
        def sigterm_receiver():
            def handle_sigterm(signum, frame):
                setproctitle("simpleflow worker: shutting down")
                time.sleep(10)
                os._exit(0)
            signal.signal(signal.SIGTERM, handle_sigterm)
            setproctitle("simpleflow worker: running")
            time.sleep(60)

        supervisor = Supervisor(sigterm_receiver, nb_children=1)
        supervisor.start()

        # TODO: find a non-sleep approach
        time.sleep(1)
        self.assertProcess(r'worker: running')

        supervisor_process = Process().children()[0]
        print supervisor_process.pid
        print os.kill(supervisor_process.pid, signal.SIGTERM)

        # TODO: find a non-sleep approach
        time.sleep(1)
        self.assertProcess(r'worker: shutting down')
