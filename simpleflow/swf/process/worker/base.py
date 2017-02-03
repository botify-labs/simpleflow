import json
import logging
import multiprocessing
import os
import traceback

import psutil
import swf.actors
import swf.exceptions
import swf.format
from simpleflow.process import Supervisor, with_state
from simpleflow.swf.process import Poller
from simpleflow.swf.task import ActivityTask
from simpleflow.swf.utils import sanitize_activity_context
from simpleflow.utils import json_dumps

from .dispatch import dynamic_dispatcher

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
    def __init__(self, domain, task_list, heartbeat=60):
        """

        :param domain:
        :type domain:
        :param task_list:
        :type task_list:
        :param heartbeat:
        :type heartbeat:
        """
        self.nb_retries = 3
        # heartbeat=0 is a special value to disable heartbeating. We want to
        # replace it by None because multiprocessing.Process.join() treats
        # this as "no timeout"
        self._heartbeat = heartbeat or None

        super(ActivityPoller, self).__init__(domain, task_list)

    @property
    def name(self):
        return '{}(task_list={})'.format(
            self.__class__.__name__,
            self.task_list,
        )

    @with_state('polling')
    def poll(self, task_list=None, identity=None):
        return swf.actors.ActivityWorker.poll(self, task_list, identity)

    @with_state('processing')
    def process(self, request):
        """
        Process a request.
        :param request:
        :type request: (str, swf.models.ActivityTask)
        """
        token, task = request
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
                reason=swf.format.reason(reason),
                details=swf.format.details(details),
            )
        except Exception as err:
            logger.error('cannot fail task {}: {}'.format(
                task.activity_type.name,
                err,
            ))


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
        activity = self.dispatch(task)
        input = json.loads(task.input)
        args = input.get('args', ())
        kwargs = input.get('kwargs', {})
        context = sanitize_activity_context(task.context)
        try:
            result = ActivityTask(activity, *args, context=context, **kwargs).execute()
        except Exception as err:
            logger.exception("process error: {}".format(str(err)))
            tb = traceback.format_exc()
            return poller.fail_with_retry(token, task, reason=str(err), details=tb)

        try:
            poller.complete_with_retry(token, json_dumps(result))
        except Exception as err:
            logger.exception("complete error")
            reason = 'cannot complete task {}: {}'.format(
                task.activity_id,
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
            # The subprocess is responsible for completing the task.
            # Either the task or the workflow execution no longer exists.
            logger.debug('heartbeat failed: {}'.format(error))
            # TODO: kill the worker at this point but make it configurable.
            return
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
