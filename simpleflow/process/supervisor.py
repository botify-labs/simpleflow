import functools
import multiprocessing
import os
import signal
import time
import types

import psutil

from simpleflow import logger
from .named_mixin import NamedMixin, with_state


def reset_signal_handlers(func):
    """
    Decorator that resets signal handlers from the decorated function. Useful
    for workers where we actively want handlers defined on the supervisor to be
    removed, because they wouldn't work on the worker process.
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        return func(*args, **kwargs)

    wrapped.__wrapped__ = func
    return wrapped


def _void_handle_sigchld(signum, frame):
    """
    Default action for a SIGCHLD signal handling is to ignore it
    which in practice has no effect on the running program. Having
    a handler that does nothing is a bit different, in the sense
    it will interrupt the execution of any "time.sleep()" routing.
    From "time" module docs:

        The actual suspension time may be less than that requested
        because any caught signal will terminate the sleep()
        following execution of that signal's catching routine.

    """
    pass


class Supervisor(NamedMixin):
    """
    The `Supervisor` class is responsible for managing one or many worker processes
    in parallel. Those processes can be "deciders" or "activity workers" in the
    SWF terminology.

    It's heavily inspired by the process Supervisor from honcho (which is a clone of
    the "foreman" process manager, in python): https://github.com/nickstenning/honcho
    It also has its roots in the former simpleflow process manager and some of Botify
    private code which wasn't really well tested, and was re-written in a TDD-y
    style.
    """

    def __init__(self, payload, arguments=None, nb_children=None, background=False):
        """
        Initializes a Manager() instance, with a payload (a callable that will be
        executed on worker processes), some arguments (a list or tuple of arguments
        to pass to the callable on workers), and nb_children (the expected number
        of workers, which defaults to the number of CPU cores if not passed).

        :param payload:
        :type payload: callable
        :param arguments:
        :type arguments: tuple | list
        :param nb_children:
        :type nb_children: int
        :param background: wether the supervisor process should launch in background
        :type background: bool
        """
        # NB: below, compare explicitly to "None" there because nb_children could be 0
        if nb_children is None:
            self._nb_children = multiprocessing.cpu_count()
        else:
            self._nb_children = nb_children
        self._payload = payload
        self._payload_friendly_name = self.payload_friendly_name()
        self._named_mixin_properties = ["_payload_friendly_name", "_nb_children"]
        self._args = arguments if arguments is not None else ()
        self._background = background

        self._processes = {}
        self._terminating = False

        super(Supervisor, self).__init__()

    @with_state("running")
    def start(self):
        """
        Used to start the Supervisor process once it's configured. Has to be called
        explicitly on a Supervisor instance so it starts (no auto-start from __init__()).
        """
        logger.info('starting {}'.format(self._payload))
        if self._background:
            p = multiprocessing.Process(target=self.target)
            p.start()
        else:
            self.target()

    def _cleanup_worker_processes(self):
        # cleanup children
        to_remove = []
        for pid, child in self._processes.items():
            try:
                name, status = child.name(), child.status()
            except psutil.NoSuchProcess:  # May be untimely deceased
                name, status = "unknown", "unknown"
            logger.debug("  child: name=%s pid=%d status=%s" % (name, child.pid, status))
            if status in (psutil.STATUS_ZOMBIE, "unknown"):
                logger.debug("  process {} is zombie, will cleanup".format(child.pid))
                # join process to clean it up
                child.wait()
                # set the process to be removed from self._processes
                to_remove.append(pid)

        # cleanup our internal state (self._processes)
        for pid in to_remove:
            del self._processes[pid]

    def _start_worker_processes(self):
        """
        Start missing worker processes depending on self._nb_children and the current
        processes stored in self._processes.
        """
        if self._terminating:
            return
        for _ in range(len(self._processes), self._nb_children):
            child = multiprocessing.Process(
                target=reset_signal_handlers(self._payload),
                args=self._args
            )
            child.start()

            # One might wonder if `child.pid` is guaranteed to be set at this
            # point. I tried it experimentally, and read quickly the source
            # at https://github.com/python/cpython/blob/2.7/Lib/multiprocessing/process.py
            # which shows that `pid` ultimately translates to `os.getpid()` after the
            # fork. So no big risk, but I add an assertion just in case anyway.
            pid = child.pid
            assert pid, "Cannot add process with pid={}: {}".format(pid, child)
            self._processes[pid] = psutil.Process(pid)

    def target(self):
        """
        Supervisor's main "target", as defined in the `multiprocessing` API. It's the
        code that the manager will execute once started.
        """
        # handle signals
        self.bind_signal_handlers()

        # protection against double use of ".start()"
        if len(self._processes) != 0:
            raise Exception("Child processes map is not empty, already called .start() ?")

        # wait for all processes to finish
        while True:
            # if terminating, join all processes and exit the loop so we finish
            # the supervisor process
            if self._terminating:
                for proc in self._processes.values():
                    logger.info("process: waiting for proces={} to finish.".format(proc))
                    proc.wait()
                break

            # start worker processes
            self._cleanup_worker_processes()
            self._start_worker_processes()

            # re-evaluate state at least every 5 seconds ; if a SIGCHLD happens during
            # the "time.sleep()" below, it will be interrupted, making the code above
            # run nearly immediately ; but if a SIGCHLD happens during the two calls
            # above, the "time.sleep()" here won't be stopped, so better have it
            # relatively short, but not too short since the above methods involve
            # scanning a bunch of entries in /proc so that could become slow if we do
            # it every 0.1s.
            time.sleep(5)

    def bind_signal_handlers(self):
        """
        Binds signals for graceful shutdown:
        - SIGTERM and SIGINT lead to a graceful shutdown
        - SIGCHLD is intentionally left to a void handler, see comment
        - other signals are not modified for now
        """

        # NB: Function is nested to have a reference to *self*.
        def _handle_graceful_shutdown(signum, frame):
            signals_map = {2: "SIGINT", 15: "SIGTERM"}
            signal_name = signals_map.get(signum, signum)
            logger.info("process: caught signal signal={} pid={}".format(
                signal_name, os.getpid()))
            self.terminate()

        # bind SIGTERM and SIGINT
        signal.signal(signal.SIGTERM, _handle_graceful_shutdown)
        signal.signal(signal.SIGINT, _handle_graceful_shutdown)

        # bind SIGCHLD
        signal.signal(signal.SIGCHLD, _void_handle_sigchld)

    @with_state("stopping")
    def terminate(self):
        """
        Terminate all worker processes managed by this Supervisor.
        """
        self._terminating = True
        logger.info(
            "process: will stop workers, this might take up several minutes. "
            "Please, be patient."
        )
        self._killall()

    def _killall(self):
        """
        Sends a stop (SIGTERM) signal to all worker processes.
        """
        for process in self._processes.values():
            logger.info("process: sending SIGTERM to pid={}".format(process.pid))
            process.terminate()

    def payload_friendly_name(self):
        payload = self._payload
        if isinstance(payload, types.MethodType):
            instance = payload.__self__
            return "{}.{}".format(instance.__class__.__name__, payload.__name__)
        elif isinstance(payload, types.FunctionType):
            return payload.__name__
        raise TypeError('invalid payload type {}'.format(type(payload)))
