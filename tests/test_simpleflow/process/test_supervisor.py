from __future__ import annotations

import multiprocessing
import os
import signal
import sys
import time

from flaky import flaky
from psutil import Process
from pytest import mark
from setproctitle import setproctitle
from sure import expect

from simpleflow.process import Supervisor, reset_signal_handlers
from tests.utils import IntegrationTestCase

TIME_STORE = {}


def increase_wait_time(err, func_name, func, plugin):
    """
    This function is used as a "rerun_filter" for "flaky". It increases an offset
    time in TIME_STORE that can be used later inside tests, and is actually used
    in TestSupervisor.wait() to wait more and more in case we run tests on a slow
    machine.
    """
    # offset time starts at 0 on first invocation
    TIME_STORE[func_name] = TIME_STORE.get(func_name, -1) + 1
    return True


@flaky(max_runs=8, rerun_filter=increase_wait_time)
class TestSupervisor(IntegrationTestCase):
    def wait(self, seconds=0.5):
        caller = sys._getframe(1).f_code.co_name
        wait_offset = TIME_STORE.get(caller, 0)
        time.sleep(seconds + wait_offset)

    @mark.skip("flaky test based on time.sleep")
    # @mark.xfail(platform.system() == 'Darwin', reason="setproctitle doesn't work reliably on MacOSX")
    # @mark.xfail(platform.python_implementation() == 'PyPy', reason="this test is too flaky on pypy")
    def test_start(self):
        # dummy function used in following tests
        def sleep_long(seconds):
            setproctitle(f"simpleflow Worker(sleep_long, {seconds})")
            time.sleep(seconds)

        # create a supervisor sub-process
        supervisor = Supervisor(sleep_long, arguments=(30,), nb_children=2, background=True)
        supervisor.start()

        # we need to wait a little here so the process starts and gets its name set
        # TODO: find a non-sleep approach to this
        self.wait(0.5)
        self.assertProcess(r"simpleflow Supervisor\(_payload_friendly_name=sleep_long, _nb_children=2\)")
        self.assertProcess(r"simpleflow Worker\(sleep_long, 30\)", count=2)

    @mark.skip("flaky test based on time.sleep")
    # @mark.xfail(platform.system() == 'Darwin', reason="setproctitle doesn't work reliably on MacOSX")
    # @mark.xfail(platform.python_implementation() == 'PyPy', reason="this test is too flaky on pypy")
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

        supervisor = Supervisor(sigterm_receiver, nb_children=1, background=True)
        supervisor.start()

        # TODO: find a non-sleep approach
        self.wait(1)
        self.assertProcess(r"worker: running")

        supervisor_process = Process().children()[0]
        os.kill(supervisor_process.pid, signal.SIGTERM)

        # TODO: find a non-sleep approach
        self.wait(1)
        self.assertProcess(r"worker: shutting down")

    @mark.skip("flaky test based on time.sleep")
    # @mark.xfail(platform.system() == 'Darwin', reason="setproctitle doesn't work reliably on MacOSX")
    def test_payload_friendly_name(self):
        def foo():
            pass

        supervisor = Supervisor(foo, background=True)
        self.assertEqual(supervisor._payload_friendly_name, "foo")

        class Foo:
            def bar(self):
                pass

        supervisor = Supervisor(Foo().bar, background=True)
        self.assertEqual(supervisor._payload_friendly_name, "Foo.bar")

    @mark.skip("flaky test based on time.sleep")
    # @mark.xfail(platform.system() == 'Darwin', reason="setproctitle doesn't work reliably on MacOSX")
    def test_maintain_the_pool_of_workers_if_not_terminating(self):
        # dummy function used in following tests
        def sleep_long(seconds):
            setproctitle(f"simpleflow Worker(sleep_long, {seconds})")
            time.sleep(seconds)

        # retrieve workers (not great; TODO: move it to Supervisor class)
        def workers():
            return [p for p in Process().children(recursive=True) if "Worker(sleep_long" in p.name()]

        # create a supervisor sub-process
        supervisor = Supervisor(sleep_long, arguments=(30,), nb_children=1, background=True)
        supervisor.start()

        # we need to wait a little here so the workers start
        self.wait(0.5)
        old_workers = workers()
        expect(len(old_workers)).to.equal(1)

        # now kill the worker
        old_workers[0].pid
        os.kill(workers()[0].pid, signal.SIGKILL)
        self.wait(0.5)

        # ... and check that the process has been replaced
        new_workers = workers()
        expect(len(new_workers)).to.equal(1)
        expect(new_workers[0].pid).to.not_be.equal(old_workers[0].pid)

    # NB: not in the Supervisor class but we want to benefit from the tearDown()
    @mark.skip("flaky test based on time.sleep")
    # @mark.xfail(platform.system() == 'Darwin', reason="setproctitle doesn't work reliably on MacOSX")
    def test_reset_signal_handlers(self):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        def foo():
            self.wait(1)

        # check it ignores SIGTERM normally
        p = multiprocessing.Process(target=foo)
        p.start()
        # TODO: find a non-sleep approach to this
        self.wait(0.5)
        os.kill(p.pid, signal.SIGTERM)
        p.join()
        expect(p.exitcode).to.equal(0)

        # check it fails with the decorator (meaning that SIGTERM is not ignored
        # anymore)
        p = multiprocessing.Process(target=reset_signal_handlers(foo))
        p.start()
        # TODO: find a non-sleep approach to this
        self.wait(0.5)
        os.kill(p.pid, signal.SIGTERM)
        p.join()
        expect(p.exitcode).to.equal(-15)
