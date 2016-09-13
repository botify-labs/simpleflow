#!/usr/bin/env python

"""
This script is meant as a basis for simpleflow multi-process management. It's a
bit hard to think of process management inside the current simpleflow codebase,
so this example tries to be as simple as possible and only demonstrate the way
things work.
"""
import datetime
from multiprocessing import Process, Value
import os
import random
import signal
import time

# Utility
def log(msg):
    now = datetime.datetime.now().isoformat()
    print "{} pid={} {}".format(now, os.getpid(), msg)

# Shared value
shared_alive_value = Value('b', True)

# This is the supervisor process
class Supervisor(object):
    def __init__(self, payload):
        self._is_alive = shared_alive_value
        self._nb_children = 4
        self._processes = []
        self._payload = payload
        self._is_manager = False
        self.bind_signal_handlers()

    def start(self):
        log("starting supervisor")
        for _ in xrange(self._nb_children):
            child = Process(
                target=self._payload,
            )
            child.start()
            self._processes.append(child)
        for proc in self.processes:
            proc.join()
        self._is_manager = True

    @property
    def is_manager(self):
        # self._is_manager is being set *after* we started all subprocesses,
        # so by security we should also check if self.processes has any item
        return self._is_manager or self.processes

    @property
    def is_alive(self):
        return self._is_alive.value

    def bind_signal_handlers(self):
        # NB: the function below is nested to have a reference to *self*
        def signal_graceful_shutdown(signum, frame):
            if not self.is_alive: # children
                return
            log("received signal={}, will shutdown".format(signum))
            self.stop()
        signal.signal(signal.SIGTERM, signal_graceful_shutdown)
        signal.signal(signal.SIGINT, signal_graceful_shutdown)

    @property
    def processes(self):
        new_processes = set()
        for p in self._processes:
            try:
                if p.is_alive():
                    new_processes.add(p)
            except AssertionError:
                # The stdlib raises AssertionError's when testing a non child:
                #   AssertionError: can only test a child process
                #   AssertionError: can only join a child process
                # This can happen if the process is not alive anymore (?) OR if
                # we receive this in a child process.
                pass
        self._processes = new_processes
        return self._processes

    def stop(self):
        if not self.is_manager:
            return
        log("calling stop() on: {}".format(self))
        self._is_alive.value = False
        for p in self.processes:
            log("waiting for process to shutdown: process={} pid={}".format(p, p.pid))
            p.join()
        log("all processes shut down, exiting...")

# A dummy decider process
class Decider(object):
    def __init__(self):
        self._is_alive = shared_alive_value

    @property
    def is_alive(self):
        return self._is_alive.value

    def poll(self):
        while self.is_alive:
            sec = random.randint(3, 10)
            log("polling for task (will take {}s)".format(sec))
            # time.sleep(sec)
            # <dumb section>
            # "time.sleep()" gets interrupted when receiving a SIGINT it seems,
            # so let's spend time in a different way
            i = 0
            for _ in xrange(0, sec * 10000000):
                i = i + 1
            # </end of dumb section>
        log("loop ended")

# Let's go now
if __name__ == "__main__":
    decider = Decider()
    supervisor = Supervisor(decider.poll)
    supervisor.start()
