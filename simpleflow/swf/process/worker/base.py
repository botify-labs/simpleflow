import json
import logging
import multiprocessing
import os
import signal
import traceback

import psutil
import swf.actors
import swf.exceptions
import swf.format
from simpleflow.swf.process.actor import (
    Supervisor,
    Poller,
    with_state,
)

from .dispatch import dynamic_dispatcher

from simpleflow.swf.task import ActivityTask
from simpleflow.utils import json_dumps

logger = logging.getLogger(__name__)


class Worker(Supervisor):
    def __init__(self, poller, nb_children=None):
        self._poller = poller
        self._poller.is_alive = True
        Supervisor.__init__(
            self,
            payload=self._poller.start,
            nb_children=nb_children,
        )


class ActivityPoller(Poller, swf.actors.ActivityWorker):
    """
    Polls an activity and handles it in the worker.

    """
    def __init__(self, workflow_id, domain, task_list, heartbeat=60,
                 *args, **kwargs):
        """

        :param workflow_id:
        :type workflow_id:
        :param domain:
        :type domain:
        :param task_list:
        :type task_list:
        :param heartbeat:
        :type heartbeat:
        :param args:
        :type args:
        :param kwargs:
        :type kwargs:
        """
        self._workflow_id = workflow_id
        self.nb_retries = 3
        # heartbeat=0 is a special value to disable heartbeating. We want to
        # replace it by None because multiprocessing.Process.join() treats
        # this as "no timeout"
        self._heartbeat = heartbeat or None

        swf.actors.ActivityWorker.__init__(
            self,
            domain,
            task_list,
            *args,  # directly forward them.
            **kwargs  # directly forward them.
        )

    @property
    def name(self):
        return '{}({})'.format(
            self.__class__.__name__,
            self._workflow_id,
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
    def __init__(self):
        self._dispatcher = dynamic_dispatcher.Dispatcher()

    def dispatch(self, task):
        """

        :param task:
        :type task: swf.models.ActivityTask
        :return:
        :rtype: simpleflow.activity.Activity
        """
        name = task.activity_type.name
        return self._dispatcher.dispatch(name)

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
        try:
            result = ActivityTask(activity, *args, **kwargs).execute()
        except Exception as err:
            logger.exception("process error")
            tb = traceback.format_exc()
            return poller.fail(token, task, reason=str(err), details=tb)

        try:
            poller._complete(token, json_dumps(result))
        except Exception as err:
            logger.exception("complete error")
            reason = 'cannot complete task {}: {}'.format(
                task.activity_id,
                err,
            )
            poller.fail(token, task, reason)


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


def monitor_child(worker):
    """
    Fill the info dict at child's exit.
    :param worker:
    :type worker: multiprocessing.Process
    """

    def _handle_child_exit(signum, frame):
        if signum == signal.SIGCHLD:
            # call worker.join() to update multiprocessing's view of the process
            # (exit code, list of our children, etc.)
            try:
                worker.join(timeout=0)
            except Exception:
                # Must have been some race, ignore it
                pass

    signal.signal(signal.SIGCHLD, _handle_child_exit)


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

    monitor_child(worker)

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
                poller.fail(
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
