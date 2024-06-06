from __future__ import annotations

import json
import os
import sys
import traceback
from base64 import b64decode
from typing import TYPE_CHECKING, Any

import multiprocess
import psutil

import simpleflow.swf.mapper.actors
import simpleflow.swf.mapper.exceptions
from simpleflow import format, logger, settings
from simpleflow.dispatch import dynamic_dispatcher
from simpleflow.download import download_binaries
from simpleflow.exceptions import ExecutionError
from simpleflow.process import Supervisor, with_state
from simpleflow.swf.mapper.models.activity import ActivityTask as BaseActivityTask
from simpleflow.swf.mapper.responses import Response
from simpleflow.swf.process.poller import Poller
from simpleflow.swf.task import ActivityTask
from simpleflow.swf.utils import sanitize_activity_context
from simpleflow.utils import format_exc, format_exc_type, json_dumps

if TYPE_CHECKING:
    from simpleflow.activity import Activity
    from simpleflow.swf.mapper.models.domain import Domain


class Worker(Supervisor):
    def __init__(self, poller, nb_children=None):
        self._poller = poller
        super().__init__(
            payload=self._poller.start,
            nb_children=nb_children,
        )


class ActivityPoller(Poller, simpleflow.swf.mapper.actors.ActivityWorker):
    """
    Polls an activity and handles it in the worker.
    """

    def __init__(
        self,
        domain: Domain,
        task_list: str | None,
        middlewares: dict[str, list[str]] | None = None,
        heartbeat: int = 60,
        poll_data: str | None = None,
    ) -> None:
        """
        :param middlewares: Paths to middleware functions to execute before and after any Activity
        :param process_mode: Whether to process locally (default)
        """
        self.nb_retries = 3
        # heartbeat=0 is a special value to disable heartbeating. We want to
        # replace it by None because multiprocessing.Process.join() treats
        # this as "no timeout"
        self._heartbeat = heartbeat or None
        self.middlewares = middlewares

        self.poll_data = poll_data
        super().__init__(domain, task_list)

    @property
    def name(self):
        return f"{self.__class__.__name__}(task_list={self.task_list})"

    @with_state("polling")
    def poll(self, task_list: str | None = None, identity: str | None = None) -> Response:
        if self.poll_data:
            # the poll data has been passed as input
            return self.fake_poll()
        else:
            # we need to poll SWF's PollForActivityTask
            return simpleflow.swf.mapper.actors.ActivityWorker.poll(self, task_list, identity)

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
    def process(self, response: Response) -> None:
        """
        Process a simpleflow.swf.mapper.actors.ActivityWorker poll response.
        """
        token = response.task_token
        task = response.activity_task
        spawn(self, token, task, self.middlewares, self._heartbeat)

    @with_state("completing")
    def complete(self, token: str, result: str | None = None) -> None:
        simpleflow.swf.mapper.actors.ActivityWorker.complete(self, token, result)

    # noinspection PyMethodOverriding
    @with_state("failing")
    def fail(
        self, token: str, task: ActivityTask, reason: str | None = None, details: str | None = None
    ) -> dict[str, Any] | None:
        """
        Fail the activity, log and ignore exceptions.
        """
        try:
            return simpleflow.swf.mapper.actors.ActivityWorker.fail(
                self,
                token,
                reason=reason,
                details=details,
            )
        except Exception as err:
            logger.error(f"cannot fail task {task.activity_type.name}: {err}")


class ActivityWorker:
    def __init__(self, dispatcher=None):
        self._dispatcher = dispatcher or dynamic_dispatcher.Dispatcher()

    def dispatch(self, task: ActivityTask) -> Activity:
        name = task.activity_type.name
        return self._dispatcher.dispatch_activity(name)

    def process(
        self, poller: ActivityPoller, token: str, task: ActivityTask, middlewares: dict[str, list[str]] | None = None
    ) -> Any:
        logger.debug("ActivityWorker.process()")
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
            logger.exception(f"process error: {exc_value!s}")
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
            logger.info("completing activity id=%s", task.activity_id)
            poller.complete_with_retry(token, result)
        except Exception as err:
            logger.exception("failed to complete activity id=%s", task.activity_id)
            reason = f"cannot complete task {task.activity_id}: {err.__class__.__name__} {err}"
            poller.fail_with_retry(token, task, reason)


def process_task(poller, token: str, task: ActivityTask, middlewares: dict[str, list[str]] | None = None) -> None:
    logger.debug("process_task()")
    format.JUMBO_FIELDS_MEMORY_CACHE.clear()
    worker = ActivityWorker()
    worker.process(poller, token, task, middlewares)


def reap_process_tree(pid: int, wait_timeout: float = settings.ACTIVITY_SIGTERM_WAIT_SEC) -> None:
    """
    TERMinates (and KILLs) if necessary a process and its descendants.

    See also: https://psutil.readthedocs.io/en/latest/#kill-process-tree.
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
        logger.warning(f"process: pid={p.pid} status={p.status()} did not respond to SIGTERM. Trying SIGKILL")
        try:
            p.kill()
        except psutil.NoSuchProcess:
            pass
    # Check
    _, alive = psutil.wait_procs(alive)
    for p in alive:
        logger.error(f"process: pid={p.pid} status={p.status()} still alive. Giving up!")


def spawn(
    poller: ActivityPoller,
    token: str,
    task: ActivityTask,
    middlewares: dict[str, list[str]] | None = None,
    heartbeat: int = 60,
) -> None:
    """
    Spawn a process and wait for it to end, sending heartbeats to SWF.

    On activity timeouts and termination, we reap the worker process and its
    children.
    """
    logger.info("spawning new activity id=%s worker heartbeat=%s", task.activity_id, heartbeat)
    worker = multiprocess.Process(target=process_task, args=(poller, token, task, middlewares))
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
                    logger.warning(f"process {worker.pid} is dead but multiprocess doesn't know it (simpleflow bug)")
            if worker.exitcode != 0:
                poller.fail_with_retry(
                    token,
                    task,
                    reason=f"process {worker.pid} died: exit code {worker.exitcode}",
                )
            return
        try:
            logger.debug(f"heartbeating for pid={worker.pid} (token={token})")
            response = poller.heartbeat(token)
        except simpleflow.swf.mapper.exceptions.DoesNotExistError as error:
            # Either the task or the workflow execution no longer exists,
            # let's kill the worker process.
            logger.warning(f"heartbeat failed: {error}")
            logger.warning(f"killing (KILL) worker with pid={worker.pid}")
            reap_process_tree(worker.pid)
            return
        except simpleflow.swf.mapper.exceptions.RateLimitExceededError as error:
            # ignore rate limit errors: high chances the next heartbeat will be
            # ok anyway, so it would be stupid to break the task for that
            logger.warning(
                f'got a "ThrottlingException / Rate exceeded" when heartbeating for task {task.activity_type.name}:'
                f" {error}"
            )
            continue
        except Exception as error:
            # Let's crash if it cannot notify the heartbeat failed.  The
            # subprocess will become orphan and the heartbeat timeout may
            # eventually trigger on Amazon SWF side.
            logger.error(f"cannot send heartbeat for task {task.activity_type.name}: {error}")
            raise

        # Task cancelled.
        if response and response.get("cancelRequested"):
            reap_process_tree(worker.pid)
            return
