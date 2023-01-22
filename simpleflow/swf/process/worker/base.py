from __future__ import annotations

import json
import multiprocessing
import os
import sys
import traceback
import uuid
from base64 import b64decode

import psutil

import swf.actors
import swf.exceptions
from simpleflow import format, logger, settings
from simpleflow.dispatch import dynamic_dispatcher
from simpleflow.download import download_binaries
from simpleflow.exceptions import ExecutionError
from simpleflow.job import KubernetesJob
from simpleflow.process import Supervisor, with_state
from simpleflow.swf.constants import VALID_PROCESS_MODES
from simpleflow.swf.process import Poller
from simpleflow.swf.task import ActivityTask
from simpleflow.swf.utils import sanitize_activity_context
from simpleflow.utils import format_exc, format_exc_type, json_dumps, to_k8s_identifier
from swf.models import ActivityTask as BaseActivityTask
from swf.responses import Response


class Worker(Supervisor):
    def __init__(self, poller, nb_children=None):
        self._poller = poller
        super().__init__(
            payload=self._poller.start,
            nb_children=nb_children,
        )


class ActivityPoller(Poller, swf.actors.ActivityWorker):
    """
    Polls an activity and handles it in the worker.

    """

    def __init__(
        self,
        domain,
        task_list,
        middlewares=None,
        heartbeat=60,
        process_mode=None,
        poll_data=None,
    ):
        """

        :param domain:
        :type domain:
        :param task_list:
        :type task_list:
        :param middlewares: Paths to middleware functions to execute before and after any Activity
        :type middlewares: Optional[Dict[str, str]]
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
        self.middlewares = middlewares

        self.process_mode = process_mode or "local"
        if self.process_mode not in VALID_PROCESS_MODES:
            raise AssertionError(f'invalid process_mode "{self.process_mode}"')

        self.poll_data = poll_data
        super().__init__(domain, task_list)

    @property
    def name(self):
        return f"{self.__class__.__name__}(task_list={self.task_list})"

    @with_state("polling")
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

    @with_state("processing")
    def process(self, response):
        """
        Process a swf.actors.ActivityWorker poll response..
        :param response:
        :type response: swf.responses.Response
        """
        token = response.task_token
        task = response.activity_task
        if self.process_mode == "kubernetes":
            try:
                spawn_kubernetes_job(self, response.raw_response)
            except Exception as err:
                logger.exception("spawn_kubernetes_job error")
                reason = "cannot spawn kubernetes job for task {}: {} {}".format(
                    task.activity_id,
                    err.__class__.__name__,
                    err,
                )
                self.fail_with_retry(token, task, reason)
        else:
            spawn(self, token, task, self.middlewares, self._heartbeat)

    @with_state("completing")
    def complete(self, token, result=None):
        swf.actors.ActivityWorker.complete(self, token, result)

    # noinspection PyMethodOverriding
    @with_state("failing")
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
            logger.error(f"cannot fail task {task.activity_type.name}: {err}")

    @property
    def identity(self):
        if self.process_mode == "kubernetes":
            self.job_name = "{}--{}".format(to_k8s_identifier(self.task_list), str(uuid.uuid4()))
            return json_dumps(
                {
                    "cluster": os.environ["K8S_CLUSTER"],
                    "namespace": os.environ["K8S_NAMESPACE"],
                    "job": self.job_name,
                }
            )
        else:
            return super().identity


class ActivityWorker:
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

    def process(self, poller, token, task, middlewares=None):
        """

        :param poller:
        :type poller: ActivityPoller
        :param token:
        :type token: str
        :param task:
        :type task: swf.models.ActivityTask
        :param middlewares: Paths to middleware functions to execute before and after any Activity
        :type middlewares: Optional[Dict[str, str]]
        """
        logger.debug(f"ActivityWorker.process() pid={os.getpid()}")
        try:
            activity = self.dispatch(task)
            input = format.decode(task.input)
            args = input.get("args", ())
            kwargs = input.get("kwargs", {})
            context = sanitize_activity_context(task.context)
            context["domain_name"] = poller.domain.name
            if input.get("meta", {}).get("binaries"):
                download_binaries(input["meta"]["binaries"])
            result = ActivityTask(
                activity,
                *args,
                context=context,
                simpleflow_middlewares=middlewares,
                **kwargs,
            ).execute()
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logger.exception(f"process error: {str(exc_value)}")
            if isinstance(exc_value, ExecutionError) and len(exc_value.args):
                details = exc_value.args[0]
                reason = format_exc(exc_value)  # FIXME json.loads and rebuild?
            else:
                tb = traceback.format_tb(exc_traceback)
                reason = format_exc(exc_value)
                details = json_dumps(
                    {
                        "error": exc_type.__name__,
                        "error_type": format_exc_type(exc_type),
                        "message": str(exc_value),
                        "traceback": tb,
                    },
                    default=repr,
                )
            return poller.fail_with_retry(token, task, reason=reason, details=details)

        try:
            logger.info("completing activity")
            poller.complete_with_retry(token, result)
        except Exception as err:
            logger.exception("complete error")
            reason = "cannot complete task {}: {} {}".format(
                task.activity_id,
                err.__class__.__name__,
                err,
            )
            poller.fail_with_retry(token, task, reason)


def process_task(poller, token, task, middlewares=None):
    """

    :param poller:
    :type poller: ActivityPoller
    :param token:
    :type token: str
    :param task:
    :type task: swf.models.ActivityTask
    :param middlewares: Paths to middleware functions to execute before and after any Activity
    :type middlewares: Optional[Dict[str, str]]
    """
    logger.debug(f"process_task() pid={os.getpid()}")
    format.JUMBO_FIELDS_MEMORY_CACHE.clear()
    worker = ActivityWorker()
    worker.process(poller, token, task, middlewares)


def spawn_kubernetes_job(poller, swf_response):
    logger.info(f"scheduling new kubernetes job name={poller.job_name}")
    job = KubernetesJob(poller.job_name, poller.domain.name, swf_response)
    job.schedule()


def reap_process_tree(pid, wait_timeout=settings.ACTIVITY_SIGTERM_WAIT_SEC):
    """
    TERMinates (and KILLs) if necessary a process and its descendants.

    See also: https://psutil.readthedocs.io/en/latest/#kill-process-tree.

    :param pid: Process ID
    :type pid: int
    :param wait_timeout: Wait timeout
    :type wait_timeout: float
    """

    def on_terminate(p):
        logger.info(f"process: terminated pid={p.pid} retcode={p.returncode}")

    if pid == os.getpid():
        raise RuntimeError("process: cannot terminate self!")
    parent = psutil.Process(pid)
    procs = parent.children(recursive=True)
    procs.append(parent)
    # Terminate
    for p in procs:
        try:
            p.terminate()
        except psutil.NoSuchProcess:
            pass
    _, alive = psutil.wait_procs(procs, timeout=wait_timeout, callback=on_terminate)
    # Kill
    for p in alive:
        logger.warning("process: pid={} status={} did not respond to SIGTERM. Trying SIGKILL".format(p.pid, p.status()))
        try:
            p.kill()
        except psutil.NoSuchProcess:
            pass
    # Check
    _, alive = psutil.wait_procs(alive)
    for p in alive:
        logger.error("process: pid={} status={} still alive. Giving up!".format(p.pid, p.status()))


def spawn(poller, token, task, middlewares=None, heartbeat=60):
    """
    Spawn a process and wait for it to end, sending heartbeats to SWF.

    On activity timeouts and termination, we reap the worker process and its
    children.

    :param poller:
    :type poller: ActivityPoller
    :param token:
    :type token: str
    :param task:
    :type task: swf.models.ActivityTask
    :param middlewares: Paths to middleware functions to execute before and after any Activity
    :type middlewares: Optional[Dict[str, str]]
    :param heartbeat: heartbeat delay (seconds)
    :type heartbeat: int
    """
    logger.info("spawning new activity worker pid={} heartbeat={}".format(os.getpid(), heartbeat))
    worker = multiprocessing.Process(target=process_task, args=(poller, token, task, middlewares))
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
                    logger.warning(
                        "process {} is dead but multiprocessing doesn't know it (simpleflow bug)".format(worker.pid)
                    )
            if worker.exitcode != 0:
                poller.fail_with_retry(
                    token,
                    task,
                    reason="process {} died: exit code {}".format(worker.pid, worker.exitcode),
                )
            return
        try:
            logger.debug(f"heartbeating for pid={worker.pid} (token={token})")
            response = poller.heartbeat(token)
        except swf.exceptions.DoesNotExistError as error:
            # Either the task or the workflow execution no longer exists,
            # let's kill the worker process.
            logger.warning(f"heartbeat failed: {error}")
            logger.warning(f"killing (KILL) worker with pid={worker.pid}")
            reap_process_tree(worker.pid)
            return
        except swf.exceptions.RateLimitExceededError as error:
            # ignore rate limit errors: high chances the next heartbeat will be
            # ok anyway, so it would be stupid to break the task for that
            logger.warning(
                'got a "ThrottlingException / Rate exceeded" when heartbeating for task {}: {}'.format(
                    task.activity_type.name, error
                )
            )
            continue
        except Exception as error:
            # Let's crash if it cannot notify the heartbeat failed.  The
            # subprocess will become orphan and the heartbeat timeout may
            # eventually trigger on Amazon SWF side.
            logger.error("cannot send heartbeat for task {}: {}".format(task.activity_type.name, error))
            raise

        # Task cancelled.
        if response and response.get("cancelRequested"):
            reap_process_tree(worker.pid)
            return
