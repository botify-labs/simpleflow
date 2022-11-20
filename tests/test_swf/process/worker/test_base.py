from __future__ import annotations

import os
import signal
import sys
import time
from multiprocessing import Lock, Process, Value

import psutil
import pytest

import simpleflow.swf.process.worker.base as base


def noop_target(handler):
    """
    Noop process target that can register a SIGTERM handler
    """
    signal.signal(signal.SIGTERM, handler)
    while True:
        time.sleep(1)


def exit_handler(signum, frame):
    sys.exit()


def noop_handler(signum, frame):
    pass


def reap_handler(signum, frame):
    base.reap_process_tree(os.getpid()) or sys.exit()


@pytest.mark.parametrize(
    "handler",
    [
        exit_handler,
        noop_handler,
    ],
)
def test_reap_process_tree_plain(handler):
    """
    Tests that process is killed when handling SIGTERM, times out, or ignores.
    """
    proc = Process(target=noop_target, args=(handler,))
    try:
        proc.start()
        # Wait until ready
        while not proc.pid:
            time.sleep(0.1)
        assert psutil.pid_exists(proc.pid)
        base.reap_process_tree(proc.pid, wait_timeout=0.1)
        assert not psutil.pid_exists(proc.pid)
    finally:
        # Clean up any potentially danging processp
        if proc.pid and psutil.pid_exists(proc.pid):
            os.kill(proc.pid, signal.SIGKILL)
            assert False, f"KILLed process with pid={proc.pid}"


def nested_target(handler, child_pid, lock):
    """
    Noop process target that can register a SIGTERM handler.

    :param handler: SIGTERM handler
    :type handler: func
    :param child_pid: child process PID
    :type child_pid: multiprocessing.Value
    :param lock: lock
    :type lock: multiprocessing.Lock
    """
    signal.signal(signal.SIGTERM, handler)
    proc = Process(target=noop_target, args=(handler,))
    proc.start()
    while not proc.pid:
        time.sleep(0.1)
    with lock:
        child_pid.value = proc.pid
    while True:
        time.sleep(1)


@pytest.mark.parametrize(
    "handler",
    [
        exit_handler,
        noop_handler,
        reap_handler,
    ],
)
def test_reap_process_tree_children(handler):
    """
    Tests recursive termination children with SIGTERM handlers.
    """
    child_pid = Value("i", 0)
    lock = Lock()
    proc = Process(target=nested_target, args=(handler, child_pid, lock))
    pids = []
    try:
        proc.start()
        while not proc.pid or not child_pid.value:
            time.sleep(0.1)
        pids = [proc.pid, child_pid.value]
        assert all(psutil.pid_exists(p) for p in pids)
        base.reap_process_tree(proc.pid, wait_timeout=1)
        assert all(not psutil.pid_exists(p) for p in pids)
    finally:
        for pid in pids:
            if pid and psutil.pid_exists(proc.pid):
                os.kill(proc.pid, signal.SIGKILL)
                assert False, f"KILLed process with pid={proc.pid}"
