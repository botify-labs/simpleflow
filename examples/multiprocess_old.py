#!/usr/bin/env python

"""
This script is meant as a basis for simpleflow multi-process management. It's a
bit hard to think of process management inside the current simpleflow codebase,
so this example tries to be as simple as possible and only demonstrate the way
things work.
"""
import datetime
import multiprocessing
import os
import random
import signal
import time

# Utility
def log(msg):
    now = datetime.datetime.now().isoformat()
    print "{} pid={} {}".format(now, os.getpid(), msg)


# Model for multi process
class MultiProcessActor(object):
    """Multi-processing implementation of a swf actor

    Implements the whole subprocess creation/management/deletion
    actions. Binds signal handlers to expected behaviors. Adds
    a stop() method to inheriting actors.
    """
    def __init__(self):
        self._is_alive = False
        self._nb_children = 4
        self._semaphore = multiprocessing.Semaphore(self._nb_children)
        self._processes = set()

        self.bind_signals_handler()

        super(MultiProcessActor, self).__init__()

    @property
    def is_alive(self):
        return self._is_alive

    @property
    def nb_children(self):
        return self._nb_children

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

    def bind_signals_handler(self):
        """
        Binds SIGTERM and SIGINT to actors graceful shutdown action, and SIGUSR1 to
        a debug action to display process hierarchy + some useful informations.
        """
        def sigtermhandler(signum, frame):
            if self.is_alive:
                log(
                    "process: signal caught ({}), shutting down process {} (pid={}) "
                    "Might take up several minutes. Please, be patient.".format(
                        signum, self.name, os.getpid())
                )
                self._is_alive = False
                self.stop()
                log("process: succesfully shut down {} and subprocesses".format(self.name))
            else:
                log(
                    "process: signal caught ({}), but process {} is already shutting "
                    "down, or not started yet.".format(signum, os.getpid())
                )

        # Bind SIGTERM and SIGINT to sigtermhandler
        signal.signal(signal.SIGTERM, sigtermhandler)
        signal.signal(signal.SIGINT, sigtermhandler)

    def stop(self, graceful=True, join_timeout=None):
        log('stopping {} (pid={})'.format(self.name, os.getpid()))
        if graceful:
            for p in self.processes:
                log("process: waiting for subprocess {} (pid={}) to finish "
                    "(with timeout={}s)".format(self.name, p.pid, join_timeout))
                p.join(join_timeout)
        else:
            for p in self.processes:
                log("process: terminating subprocess {} (pid={}) ".format(self.name, p.pid))
                p.terminate()
        log("all processes shut down, exiting...")
        self._is_alive = False

# A dummy decider process
class Decider(MultiProcessActor):
    def __init__(self):
        self.name = "decider"
        MultiProcessActor.__init__(self)

    def spawn_handler(self):
        try:
            self._semaphore.acquire()
        except OSError, err:
            log('Error: cannot acquire semaphore: {}'.format(err))
            return

        if self.is_alive:
            process = multiprocessing.Process(target=self.poll)
            process.start()
            self.processes.add(process)

    def poll(self):
        sec = random.randint(3, 10)
        log("polling for task (will take {}s)".format(sec))
        try:
            # time.sleep(sec)
            # <dumb section>
            # "time.sleep()" gets interrupted when receiving a SIGINT it seems,
            # so let's spend time in a different way
            i = 0
            for _ in xrange(0, sec * 10000000):
                i = i + 1
            # </end of dumb section>
        finally:
            try:
                self._semaphore.release()
            except Exception, err:
                log('Error: cannot release semaphore: {}'.format(err))

    def start(self):
        self._is_alive = True
        while self.is_alive:
            self.spawn_handler()
        log("loop ended")


if __name__ == "__main__":
    log("starting supervisor")
    d = Decider()
    d.start()
