import abc
import functools
import logging
import multiprocessing
import signal
from builtins import range

from setproctitle import setproctitle

import swf.actors
import swf.exceptions
from simpleflow import utils
from simpleflow.process import NamedMixin, with_state
from simpleflow.swf.helpers import swf_identity


logger = logging.getLogger(__name__)


__all__ = ['Poller']


def reset_signal_handlers(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        return func(*args, **kwargs)

    wrapped.__wrapped__ = func
    return wrapped


class Poller(NamedMixin, swf.actors.Actor):
    """Multi-processing implementation of a SWF actor.

    """
    def __init__(self,
                 domain,
                 task_list=None,
                 *args, **kwargs):
        self.is_alive = False
        self._named_mixin_properties = ["task_list"]

        swf.actors.Actor.__init__(self, domain, task_list)
        NamedMixin.__init__(self)

    @property
    def identity(self):
        """Identity when polling decision task.

        http://docs.aws.amazon.com/amazonswf/latest/apireference/API_PollForDecisionTask.html

        Identity of the decider making the request, which is recorded in the
        DecisionTaskStarted event in the workflow history. This enables
        diagnostic tracing when problems arise. The form of this identity is
        user defined. Minimum length of 0. Maximum length of 256.

        """
        return swf_identity()

    def stop_gracefully(self, join_timeout=60):
        self._worker.join(join_timeout)

    def stop_forcefully(self):
        self._worker.terminate()

    @with_state('running')
    def start(self):
        """
        Start the main decider process. There is no daemonization. The process
        is intended to be run inside a supervisor process.

        """
        logger.info("starting %s on domain %s", self.name, self.domain.name)
        self.set_process_name()
        while self.is_alive:
            try:
                response = self._poll()
            except swf.exceptions.PollTimeout:
                continue
            self.process(response)

    @with_state('stopping')
    def stop(self, graceful=True, join_timeout=60):
        """Stop the actor processes and subprocesses.

        :param graceful: wait for children processes?
        :type  graceful: bool.
        :param join_timeout: maximum time to wait for children.
        :type  join_timeout: int.
        """
        logger.info('stopping %s', self.name)
        self.is_alive = False  # No longer take requests.

        if graceful:
            self.stop_gracefully(join_timeout)
        else:
            self.stop_forcefully()

    def _complete(self, token, response):
        """
        Complete with retry.
        :param token:
        :type token: str
        :param response: response: decision list, JSON result, ...
        :type response: Any
        :return:
        :rtype:
        """
        # FIXME this is a public member
        try:
            complete = utils.retry.with_delay(
                nb_times=self.nb_retries,
                delay=utils.retry.exponential,
                log_with=logger.exception,
                except_on=swf.exceptions.DoesNotExistError,
            )(self.complete)  # Exponential backoff on errors.
            complete(token, response)
        except Exception as err:
            # This is embarrassing because the decider cannot notify SWF of the
            # task completion. As it will not try again, the task will
            # timeout (start_to_complete).
            logger.exception("cannot complete task: %s", str(err))

    @abc.abstractmethod
    def poll(self, task_list, identity):
        raise NotImplementedError

    @abc.abstractmethod
    def complete(self, token, response):
        raise NotImplementedError

    @abc.abstractmethod
    def process(self, request):
        pass

    def _poll(self):
        """
        Polls a task represented by its token and data. It uses long-polling
        with a timeout of one minute.

        See also
        http://docs.aws.amazon.com/amazonswf/latest/apireference/API_PollForDecisionTask.html#API_PollForDecisionTask_RequestSyntax
        http://docs.aws.amazon.com/amazonswf/latest/apireference/API_PollForActivityTask.html#API_PollForActivityTask_RequestSyntax

        :returns:
        :rtype: swf.responses.Response
        """
        task_list = self.task_list
        identity = self.identity

        logger.debug("polling task on %s", task_list)
        try:
            response = self.poll(
                task_list,
                identity=identity,
            )
        except swf.exceptions.PollTimeout:
            logger.debug('{}: PollTimeout'.format(self))
            raise
        except Exception as err:
            logger.error(
                "exception %s when polling on %s",
                str(err),
                task_list,
            )
            raise
        return response
