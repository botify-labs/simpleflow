import logging
import multiprocessing
import os
import signal

from setproctitle import setproctitle


logger = logging.getLogger(__name__)


class Supervisor(object):
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
    def __init__(self, payload, arguments=None, nb_children=None):
        """
        Initializes a Manager() instance, with a payload (a callable that will be
        executed on worker processes), some arguments (a list or tuple of arguments
        to pass to the callable on workers), and nb_children (the expected number
        of workers, which defaults to the number of CPU cores if not passed).

        :param payload:
        :type payload: callable
        :param arguments:
        :type arguments: tuple, list
        :param nb_children:
        :type nb_children: int
        """
        # NB: below, compare explicitly to "None" there because nb_children could be 0
        if nb_children is None:
            self._nb_children = multiprocessing.cpu_count()
        else:
            self._nb_children = nb_children
        self._payload = payload
        self._args = arguments if arguments is not None else ()

        self._processes = []

        self._terminating = False

    def start(self):
        """
        Used to start the Supervisor process once it's configured. Has to be called
        explicitly on a Supervisor instance so it starts (no auto-start from __init__()).
        """
        logger.info('starting {}'.format(self._payload))
        # TODO: question if necessary to use a subprocess here ; it eases tests but not
        # sure it won't make things more complex down the road
        p = multiprocessing.Process(target=self.target)
        p.start()

    def target(self):
        """
        Supervisor's main "target", as defined in the `multiprocessing` API. It's the
        code that the manager will execute once started.
        """
        # handle signals
        self.bind_signal_handlers()

        # setup supervisor name
        setproctitle('simpleflow Supervisor(nb_children={})'.format(self._nb_children))

        # protection against double use of ".start()"
        if len(self._processes) != 0:
            raise Exception("Child processes list is not empty, already called .start() ?")

        # start worker processes
        for _ in range(self._nb_children):
            child = multiprocessing.Process(
                target=self._payload,
                args=self._args
            )
            child.start()
            self._processes.append(child)

        # wait for all processes to finish
        for proc in self._processes:
            proc.join()

    def bind_signal_handlers(self):
        """
        Binds signals for graceful shutdown:
        - SIGTERM and SIGINT lead to a graceful shutdown
        - other signals are ignored for now
        """
        # NB: Function is nested to have a reference to *self*.
        def _handle_graceful_shutdown(signum, frame):
            signals_map = { 2: "SIGINT", 15: "SIGTERM" }
            signal_name = signals_map.get(signum, signum)
            logger.info("process: caught signal signal={} pid={}".format(
                signal_name, os.getpid()))
            self.terminate()

        # bind SIGTERM and SIGINT
        signal.signal(signal.SIGTERM, _handle_graceful_shutdown)
        signal.signal(signal.SIGINT, _handle_graceful_shutdown)

    def terminate(self):
        """
        Terminate all worker processes managed by this Supervisor.
        """
        logger.error("processes: {}".format(self._processes))
        if self._terminating:
            logger.info("process: already shutting down...")
            return
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
        for child in self._processes:
            logger.info("process: sending SIGTERM to pid={}".format(child.pid))
            os.kill(child.pid, signal.SIGTERM)
