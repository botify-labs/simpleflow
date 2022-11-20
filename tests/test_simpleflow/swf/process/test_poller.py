from __future__ import annotations

import multiprocessing
import os
import signal
import time

from psutil import Process
from pytest import mark
from sure import expect

from simpleflow.swf.process import Poller
from swf.models import Domain
from tests.utils import IntegrationTestCase


class FakePoller(Poller):
    """
    This poller only waits 2 seconds then exits.
    """

    def poll_with_retry(self):
        # NB: time.sleep gets interrupted by any signal, so the following lines
        # are not actually as dumb as they seem to be...
        time.sleep(1)
        time.sleep(1)


class TestSupervisor(IntegrationTestCase):
    @mark.skip("flaky test based on time.sleep")
    # @mark.xfail(platform.system() == 'Darwin', reason="psutil process statuses are buggy on OSX")
    def test_sigterm_handling(self):
        """
        Tests that SIGTERM is correctly ignored by the poller.
        """
        poller = FakePoller(Domain("test-domain"), "test-task-list")
        # restore default signal handling for SIGTERM: signal binding HAS to be
        # done at execution time, not in the Poller() initialization, because in
        # the real world that step is performed at the Supervisor() level, not in
        # the worker subprocess.
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

        process = multiprocessing.Process(target=poller.start)
        process.start()
        time.sleep(0.5)

        os.kill(process.pid, signal.SIGTERM)
        time.sleep(0.5)

        # now test that we're still in the second sleep, and that we're not
        # in "zombie" mode yet (which would be the case if SIGTERM had its
        # default effect)
        expect(Process(process.pid).status()).to.contain("sleeping")
