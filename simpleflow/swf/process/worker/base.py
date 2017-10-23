from base64 import b64decode
import errno
import logging
import json
import multiprocessing
import os
import signal
import sys
import traceback
import uuid

import psutil

from simpleflow.exceptions import ExecutionError
from swf import format
import swf.actors
import swf.exceptions
from swf.models import ActivityTask as BaseActivityTask
from swf.responses import Response
from simpleflow.dispatch import dynamic_dispatcher
from simpleflow.job import KubernetesJob
from simpleflow.process import Supervisor, with_state
from simpleflow.swf.constants import VALID_PROCESS_MODES
from simpleflow.swf.process import Poller

from simpleflow.swf.task import ActivityTask
from simpleflow.swf.utils import sanitize_activity_context
from simpleflow.utils import format_exc, json_dumps, to_k8s_identifier


logger = logging.getLogger(__name__)


class Worker(Supervisor):
    def __init__(self, poller, nb_children=None):
        self._poller = poller
        super(Worker, self).__init__(
            payload=self._poller.start,
            nb_children=nb_children,
        )


class ActivityPoller(Poller, swf.actors.ActivityWorker):
    """
    Polls an activity and handles it in the worker.

    """
    def __init__(self, domain, task_list, heartbeat=60, process_mode=None, poll_data=None):
        """

        :param domain:
        :type domain:
        :param task_list:
        :type task_list:
        :param heartbeat:
        :type heartbeat:
        :param process_mode: Whether to process locally (default) or spawn a Kubernetes job.
        :type process_mode: Optional[str]
        """
        self.nb_retries = 3
        # heartbeat=0 is a special value to disable heartbeating. We want to
        # replace it by None because multiprocessing.Process.join() treats
        # this as "no timeout"
        self._heartbeat = heartbeat or None

        self.process_mode = process_mode or 'local'
        assert self.process_mode in VALID_PROCESS_MODES, 'invalid process_mode "{}"'.format(self.process_mode)

        self.poll_data = poll_data
        super(ActivityPoller, self).__init__(domain, task_list)

    @property
    def name(self):
        return '{}(task_list={})'.format(
            self.__class__.__name__,
            self.task_list,
        )

    @with_state('polling')
    def poll(self, task_list=None, identity=None):
        if self.poll_data:
            # the poll data has been passed as input
            return self.fake_poll()
        else:
            # we need to poll SWF's PollForActivityTask
            return swf.actors.ActivityWorker.poll(self, task_list, identity)

    def fake_poll(self):
        polled_activity_data = json.loads(b64decode(self.poll_data))
        activity_task = BaseActivityTask.from_poll(
            self.domain,
            self.task_list,
            polled_activity_data,
        )
        return Response(
            task_token=activity_task.task_token,
            activity_task=activity_task,
            raw_response=polled_activity_data,
        )

    @with_state('processing')
    def process(self, response):
        """
        Process a swf.actors.ActivityWorker poll response..
        :param response:
        :type response: swf.responses.Response
        """
        if self.process_mode == "kubernetes":
            job = KubernetesJob(self.job_name, self.domain.name, response.raw_response)
            job.schedule()
        else:
            token = response.task_token
            task = response.activity_task
            spawn(self, token, task, self._heartbeat)

    @with_state('completing')
    def complete(self, token, result=None):
        swf.actors.ActivityWorker.complete(self, token, result)

    # noinspection PyMethodOverriding
    @with_state('failing')
    def fail(self, token, task, reason=None, details=None):
        """
        Fail the activity, log and ignore exceptions.
        :param token:
        :type token:
        :param task:
        :type task:
        :param reason:
        :type reason:
        :param details:
        :type details:
        :return:
        :rtype:
        """
        try:
            return swf.actors.ActivityWorker.fail(
                self,
                token,
                reason=reason,
                details=details,
            )
        except Exception as err:
            logger.error('cannot fail task {}: {}'.format(
                task.activity_type.name,
                err,
            ))

    @property
    def identity(self):
        if self.process_mode == "kubernetes":
            self.job_name = "{}--{}".format(to_k8s_identifier(self.task_list), str(uuid.uuid4()))
            return json_dumps({
                "cluster": os.environ["K8S_CLUSTER"],
                "namespace": os.environ["K8S_NAMESPACE"],
                "job": self.job_name,
            })
        else:
            return super(ActivityPoller, self).identity


class ActivityWorker(object):
    def __init__(self, dispatcher=None):
        self._dispatcher = dispatcher or dynamic_dispatcher.Dispatcher()

    def dispatch(self, task):
        """

        :param task:
        :type task: swf.models.ActivityTask
        :return:
        :rtype: simpleflow.activity.Activity
        """
        name = task.activity_type.name
        return self._dispatcher.dispatch_activity(name)

    def process(self, poller, token, task):
        """

        :param poller:
        :type poller: ActivityPoller
        :param token:
        :type token: str
        :param task:
        :type task: swf.models.ActivityTask
        """
        logger.debug('ActivityWorker.process() pid={}'.format(os.getpid()))
        try:
            activity = self.dispatch(task)
            input = format.decode(task.input)
            args = input.get('args', ())
            kwargs = input.get('kwargs', {})
            context = sanitize_activity_context(task.context)
            context['domain_name'] = poller.domain.name
            result = ActivityTask(activity, *args, context=context, **kwargs).execute()
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logger.exception("process error: {}".format(str(exc_value)))
            if isinstance(exc_value, ExecutionError) and len(exc_value.args):
                details = exc_value.args[0]
                reason = format_exc(exc_value)  # FIXME json.loads and rebuild?
            else:
                tb = traceback.format_tb(exc_traceback)
                reason = format_exc(exc_value)
                details = json_dumps(
                    {
                        'error': exc_type.__name__,
                        'message': str(exc_value),
                        'traceback': tb,
                    },
                    default=repr
                )
            return poller.fail_with_retry(
                token,
                task,
                reason=reason,
                details=details
            )

        try:
            poller.complete_with_retry(token, result)
        except Exception as err:
            logger.exception("complete error")
            reason = 'cannot complete task {}: {} {}'.format(
                task.activity_id,
                err.__class__.__name__,
                err,
            )
            poller.fail_with_retry(token, task, reason)


def process_task(poller, token, task):
    """

    :param poller:
    :type poller: ActivityPoller
    :param token:
    :type token: str
    :param task:
    :type task: swf.models.ActivityTask
    """
    logger.debug('process_task() pid={}'.format(os.getpid()))
    worker = ActivityWorker()
    worker.process(poller, token, task)


def spawn(poller, token, task, heartbeat=60):
    """
    Spawn a process and wait for it to end, sending heartbeats to SWF.
    :param poller:
    :type poller: ActivityPoller
    :param token:
    :type token: str
    :param task:
    :type task: swf.models.ActivityTask
    :param heartbeat: heartbeat delay (seconds)
    :type heartbeat: int
    """
    logger.debug('spawn() pid={} heartbeat={}'.format(os.getpid(), heartbeat))
    worker = multiprocessing.Process(
        target=process_task,
        args=(poller, token, task),
    )
    worker.start()

    def worker_alive():
        return psutil.pid_exists(worker.pid)

    while worker_alive():
        worker.join(timeout=heartbeat)
        if not worker_alive():
            # Most certainly unneeded: we'll see
            if worker.exitcode is None:
                # race condition, try and re-join
                worker.join(timeout=0)
                if worker.exitcode is None:
                    logger.warning("process {} is dead but multiprocessing doesn't know it (simpleflow bug)".format(
                        worker.pid
                    ))
            if worker.exitcode != 0:
                poller.fail_with_retry(
                    token,
                    task,
                    reason='process {} died: exit code {}'.format(
                        worker.pid,
                        worker.exitcode)
                )
            return
        try:
            logger.debug(
                'heartbeating for pid={} (token={})'.format(worker.pid, token)
            )
            response = poller.heartbeat(token)
        except swf.exceptions.DoesNotExistError as error:
            # Either the task or the workflow execution no longer exists,
            # let's kill the worker process.
            logger.warning('heartbeat failed: {}'.format(error))
            logger.warning('killing (KILL) worker with pid={}'.format(worker.pid))
            try:
                # The try/except protects us from a race condition: by the
                # time we issue the os.kill() call, we're not 100% sure
                # that the worker process is still alive.
                os.kill(worker.pid, signal.SIGKILL)
            except OSError as e:
                # Compare errno to the errno for "No such process"
                if e.errno != errno.ESRCH:
                    # re-raise if we get an OSError for another reason
                    raise
                logger.warning('process was not here anymore, got OSError: {}'.format(e.strerror))
            return
        except swf.exceptions.RateLimitExceededError as error:
            # ignore rate limit errors: high chances the next heartbeat will be
            # ok anyway, so it would be stupid to break the task for that
            logger.warning(
                'got a "ThrottlingException / Rate exceeded" when heartbeating for task {}: {}'.format(
                    task.activity_type.name,
                    error))
            continue
        except Exception as error:
            # Let's crash if it cannot notify the heartbeat failed.  The
            # subprocess will become orphan and the heartbeat timeout may
            # eventually trigger on Amazon SWF side.
            logger.error('cannot send heartbeat for task {}: {}'.format(
                task.activity_type.name,
                error))
            raise

        if response and response.get('cancelRequested'):
            # Task cancelled.
            worker.terminate()  # SIGTERM
            return
