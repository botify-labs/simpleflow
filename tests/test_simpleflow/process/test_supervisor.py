import multiprocessing
import os
import re
import signal
import time
import unittest

from psutil import Process, NoSuchProcess
from setproctitle import setproctitle
from sure import expect

from simpleflow.process import Supervisor, reset_signal_handlers


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
        self.assertProcess(r'simpleflow Supervisor\(_payload_friendly_name=sleep_long, _nb_children=2\)')
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

    def test_payload_friendly_name(self):
        def foo():
            pass
        supervisor = Supervisor(foo)
        self.assertEqual(supervisor._payload_friendly_name, "foo")

        class Foo(object):
            def bar(self):
                pass
        supervisor = Supervisor(Foo().bar)
        self.assertEqual(supervisor._payload_friendly_name, "Foo.bar")

    def test_maintain_the_pool_of_workers_if_not_terminating(self):
        # dummy function used in following tests
        def sleep_long(seconds):
            setproctitle("simpleflow Worker(sleep_long, {})".format(seconds))
            time.sleep(seconds)

        # retrieve workers (not great; TODO: move it to Supervisor class)
        def workers():
            return [
                p for p in Process().children(recursive=True)
                if "Worker(sleep_long" in p.name()
            ]

        # create a supervisor sub-process
        supervisor = Supervisor(sleep_long, arguments=(30,), nb_children=1)
        supervisor.start()

        # we need to wait a little here so the workers start
        time.sleep(0.5)
        old_workers = workers()
        expect(len(old_workers)).to.equal(1)

        # now kill the worker
        worker_pid = old_workers[0].pid
        os.kill(workers()[0].pid, signal.SIGKILL)
        time.sleep(0.5)

        # ... and check that the process has been replaced
        new_workers = workers()
        expect(len(new_workers)).to.equal(1)
        expect(new_workers[0].pid).to.not_be.equal(old_workers[0].pid)

    # NB: not in the Supervisor class but we want to benefit from the tearDown()
    def test_reset_signal_handlers(self):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        def foo():
            time.sleep(1)

        # check it ignores SIGTERM normally
        p = multiprocessing.Process(target=foo)
        p.start()
        # TODO: find a non-sleep approach to this
        time.sleep(0.5)
        os.kill(p.pid, signal.SIGTERM)
        p.join()
        expect(p.exitcode).to.equal(0)

        # check it fails with the decorator (meaning that SIGTERM is not ignored
        # anymore)
        p = multiprocessing.Process(target=reset_signal_handlers(foo))
        p.start()
        # TODO: find a non-sleep approach to this
        time.sleep(0.5)
        os.kill(p.pid, signal.SIGTERM)
        p.join()
        expect(p.exitcode).to.equal(-15)
