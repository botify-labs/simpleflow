from __future__ import annotations

from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError, ReadTimeoutError

from simpleflow import format, logging_context
from simpleflow.format import JumboTooLargeError
from simpleflow.swf.mapper.actors.core import Actor
from simpleflow.swf.mapper.exceptions import (
    DoesNotExistError,
    PollTimeout,
    RateLimitExceededError,
    ResponseError,
    extract_error_code,
    extract_message,
)
from simpleflow.swf.mapper.models.activity import ActivityTask
from simpleflow.swf.mapper.responses import Response
from simpleflow.utils import format_exc

if TYPE_CHECKING:
    from simpleflow.swf.mapper.models.domain import Domain


class ActivityWorker(Actor):
    """Activity task worker actor implementation

    Once started, will start polling for activity task,
    to process, and emitting heartbeat until it's stopped
    or crashes for some reason.

    :param  domain: Domain the Actor should interact with

    :param  task_list: task list the Actor should watch for tasks on

    :param  identity: Identity of the worker making the request,
                      which is recorded in the ActivityTaskStarted
                      event in the workflow history. This enables
                      diagnostic tracing when problems arise.
                      The form of this identity is user defined.
    """

    def __init__(self, domain: Domain, task_list: str, identity: str | None = None):
        super().__init__(domain, task_list)

        self._identity = identity

    def cancel(self, task_token: str, details: str | None = None) -> dict[str, Any] | None:
        """Responds to ``swf`` that the activity task was canceled

        :param  task_token: canceled activity task token
        :param  details: provided details about cancel
        """
        try:
            return self.respond_activity_task_canceled(
                task_token,
                details=format.details(details),
            )
        except ClientError as e:
            error_code = extract_error_code(e)
            message = extract_message(e)
            if error_code == "UnknownResourceFault":
                raise DoesNotExistError(
                    f"Unable to cancel activity task with token={task_token}",
                    message,
                ) from e
            raise ResponseError(message, error_code=error_code) from e
        finally:
            logging_context.reset()

    def complete(self, task_token: str, result: Any = None) -> dict[str, Any] | None:
        """Responds to ``swf`` that the activity task is completed

        :param  task_token: completed activity task token
        :param  result: The result of the activity task.
        """
        try:
            return self.respond_activity_task_completed(
                task_token,
                format.result(result),
            )
        except ClientError as e:
            error_code = extract_error_code(e)
            message = extract_message(e)
            if error_code == "UnknownResourceFault":
                raise DoesNotExistError(
                    f"Unable to complete activity task with token={task_token}",
                    message,
                ) from e

            raise ResponseError(message, error_code=error_code) from e
        except JumboTooLargeError as e:
            return self.respond_activity_task_failed(task_token, reason=format_exc(e))

    def fail(self, task_token: str, details: str | None = None, reason: str | None = None) -> dict[str, Any] | None:
        """Replies to ``swf`` that the activity task failed

        :param  task_token: canceled activity task token
        :param  details: provided details about the failure
        :param  reason: Description of the error that may assist in diagnostics
        """
        try:
            return self.respond_activity_task_failed(
                task_token,
                details=format.details(details),
                reason=format.reason(reason),
            )
        except ClientError as e:
            error_code = extract_error_code(e)
            message = extract_message(e)
            if error_code == "UnknownResourceFault":
                raise DoesNotExistError(
                    f"Unable to fail activity task with token={task_token}",
                    message,
                ) from e

            raise ResponseError(message, error_code=error_code) from e
        except JumboTooLargeError as e:
            return self.respond_activity_task_failed(task_token, reason=format_exc(e))

    def heartbeat(self, task_token: str, details: str | None = None) -> dict[str, Any] | None:
        """Records activity task heartbeat

        :param  task_token: canceled activity task token
        :param  details: provided details about task progress
        """
        try:
            return self.record_activity_task_heartbeat(
                task_token,
                format.heartbeat_details(details),
            )
        except ClientError as e:
            error_code = extract_error_code(e)
            message = extract_message(e)
            if error_code == "UnknownResourceFault":
                raise DoesNotExistError(
                    f"Unable to send heartbeat with token={task_token}",
                    message,
                ) from e

            if error_code == "ThrottlingException":
                raise RateLimitExceededError(
                    f"Rate exceeded when sending heartbeat with token={task_token}",
                    message,
                ) from e

            raise ResponseError(message, error_code=error_code) from e

    def poll(self, task_list: str | None = None, identity: str | None = None) -> Response:
        """Polls for an activity task to process from current
        actor's instance defined ``task_list``

        if no activity task was polled, raises a PollTimeout
        exception.

        :param  task_list: task list the Actor should watch for tasks on
        :param  identity: Identity of the worker making the request,
                          which is recorded in the ActivityTaskStarted
                          event in the workflow history. This enables
                          diagnostic tracing when problems arise.
                          The form of this identity is user defined.

        :raises: PollTimeout

        :returns: task token, polled activity task
        """
        logging_context.reset()
        task_list = task_list or self.task_list
        identity = identity or self._identity

        try:
            task = self.poll_for_activity_task(
                self.domain.name,
                task_list,
                identity=format.identity(identity),
            )
        except ReadTimeoutError:
            task = {}
        except ClientError as e:
            error_code = extract_error_code(e)
            message = extract_message(e)
            if error_code == "UnknownResourceFault":
                raise DoesNotExistError(
                    "Unable to poll activity task",
                    message,
                ) from e

            raise ResponseError(message, error_code=error_code) from e

        if not task.get("taskToken"):
            raise PollTimeout("Activity Worker poll timed out")

        logging_context.set("workflow_id", task["workflowExecution"]["workflowId"])
        logging_context.set("task_type", "activity")
        logging_context.set("event_id", task["startedEventId"])
        logging_context.set("activity_id", task["activityId"])

        activity_task = ActivityTask.from_poll(
            self.domain,
            self.task_list,
            task,
        )

        return Response(
            task_token=activity_task.task_token,
            activity_task=activity_task,
            raw_response=task,
        )
