from __future__ import annotations

import boto.exception

from simpleflow import format, logging_context
from swf.actors import Actor
from swf.exceptions import (
    DoesNotExistError,
    PollTimeout,
    RateLimitExceededError,
    ResponseError,
)
from swf.models import ActivityTask
from swf.responses import Response


class ActivityWorker(Actor):
    """Activity task worker actor implementation

    Once started, will start polling for activity task,
    to process, and emitting heartbeat until it's stopped
    or crashes for some reason.

    :param  domain: Domain the Actor should interact with
    :type   domain: swf.models.Domain

    :param  task_list: task list the Actor should watch for tasks on
    :type   task_list: string

    :param  identity: Identity of the worker making the request,
                      which is recorded in the ActivityTaskStarted
                      event in the workflow history. This enables
                      diagnostic tracing when problems arise.
                      The form of this identity is user defined.
    :type   identity: string
    """

    def __init__(self, domain, task_list, identity=None):
        super().__init__(domain, task_list)

        self._identity = identity

    def cancel(self, task_token, details=None):
        """Responds to ``swf`` that the activity task was canceled

        :param  task_token: canceled activity task token
        :type   task_token: string

        :param  details: provided details about cancel
        :type   details: string
        """
        try:
            return self.connection.respond_activity_task_canceled(
                task_token,
                details=format.details(details),
            )
        except boto.exception.SWFResponseError as e:
            message = self.get_error_message(e)
            if e.error_code == "UnknownResourceFault":
                raise DoesNotExistError(
                    f"Unable to cancel activity task with token={task_token}",
                    message,
                )
            raise ResponseError(message)
        finally:
            logging_context.reset()

    def complete(self, task_token, result=None):
        """Responds to ``swf`` that the activity task is completed

        :param  task_token: completed activity task token
        :type   task_token: string

        :param  result: The result of the activity task.
        :type   result: string
        """
        try:
            return self.connection.respond_activity_task_completed(
                task_token,
                format.result(result),
            )
        except boto.exception.SWFResponseError as e:
            message = self.get_error_message(e)
            if e.error_code == "UnknownResourceFault":
                raise DoesNotExistError(
                    f"Unable to complete activity task with token={task_token}",
                    message,
                )

            raise ResponseError(message)

    def fail(self, task_token, details=None, reason=None):
        """Replies to ``swf`` that the activity task failed

        :param  task_token: canceled activity task token
        :type   task_token: string

        :param  details: provided details about the failure
        :type   details: string

        :param  reason: Description of the error that may assist in diagnostics
        :type   reason: string
        """
        try:
            return self.connection.respond_activity_task_failed(
                task_token,
                details=format.details(details),
                reason=format.reason(reason),
            )
        except boto.exception.SWFResponseError as e:
            message = self.get_error_message(e)
            if e.error_code == "UnknownResourceFault":
                raise DoesNotExistError(
                    f"Unable to fail activity task with token={task_token}",
                    message,
                )

            raise ResponseError(message)

    def heartbeat(self, task_token, details=None):
        """Records activity task heartbeat

        :param  task_token: canceled activity task token
        :type   task_token: str

        :param  details: provided details about task progress
        :type   details: string
        """
        try:
            return self.connection.record_activity_task_heartbeat(
                task_token,
                format.heartbeat_details(details),
            )
        except boto.exception.SWFResponseError as e:
            message = self.get_error_message(e)
            if e.error_code == "UnknownResourceFault":
                raise DoesNotExistError(
                    f"Unable to send heartbeat with token={task_token}",
                    message,
                )

            if e.error_code == "ThrottlingException":
                raise RateLimitExceededError(
                    "Rate exceeded when sending heartbeat with token={}".format(task_token),
                    message,
                )

            raise ResponseError(message)

    def poll(self, task_list=None, identity=None):
        """Polls for an activity task to process from current
        actor's instance defined ``task_list``

        if no activity task was polled, raises a PollTimeout
        exception.

        :param  task_list: task list the Actor should watch for tasks on
        :type   task_list: string

        :param  identity: Identity of the worker making the request,
                          which is recorded in the ActivityTaskStarted
                          event in the workflow history. This enables
                          diagnostic tracing when problems arise.
                          The form of this identity is user defined.
        :type   identity: string

        :raises: PollTimeout

        :returns: task token, polled activity task
        :rtype: (str, ActivityTask)
        """
        logging_context.reset()
        task_list = task_list or self.task_list
        identity = identity or self._identity

        try:
            task = self.connection.poll_for_activity_task(
                self.domain.name,
                task_list,
                identity=format.identity(identity),
            )
        except boto.exception.SWFResponseError as e:
            message = self.get_error_message(e)
            if e.error_code == "UnknownResourceFault":
                raise DoesNotExistError(
                    "Unable to poll activity task",
                    message,
                )

            raise ResponseError(message)

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
