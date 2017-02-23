# -*- coding: utf-8 -*-
import boto.exception

from swf.actors.core import Actor
from swf.exceptions import PollTimeout, ResponseError, DoesNotExistError
from swf.models.history import History
from swf.models.workflow import WorkflowExecution, WorkflowType
from swf.responses import Response


class Decider(Actor):
    """Decider actor implementation

    :param  domain: Domain the Actor should interact with
    :type   domain: swf.models.Domain

    :param  task_list: task list the Actor should watch for tasks on
    :type   task_list: str
    """
    def __init__(self, domain, task_list):
        super(Decider, self).__init__(
            domain,
            task_list
        )

    def complete(self, task_token,
                 decisions=None, execution_context=None):
        """Responds to ``swf`` decisions have been made about
        the task with `task_token``

        :param  task_token: completed decision task token
        :type   task_token: str

        :param  decisions: The list of decisions (possibly empty)
                           made by the decider while processing this decision task
        :type   decisions: list[swf.models.decision.base.Decision]
        :param execution_context: User-defined context to add to workflow execution.
        :type execution_context: str
        """
        try:
            self.connection.respond_decision_task_completed(
                task_token,
                decisions,
                execution_context,
            )
        except boto.exception.SWFResponseError as e:
            message = self.get_error_message(e)
            if e.error_code == 'UnknownResourceFault':
                raise DoesNotExistError(
                    "Unable to complete decision task with token={}".format(task_token),
                    message,
                )

            raise ResponseError(message)

    def poll(self, task_list=None,
             identity=None,
             **kwargs):
        """
        Polls a decision task and returns the token and the full history of the
        workflow's events.

        :param task_list: task list to poll for decision tasks from.
        :type task_list: str

        :param identity: Identity of the decider making the request,
        which is recorded in the DecisionTaskStarted event in the
        workflow history.
        :type identity: str

        :returns: a Response object with history, token, and execution set
        :rtype: swf.responses.Response

        """
        task_list = task_list or self.task_list

        task = self.connection.poll_for_decision_task(
            self.domain.name,
            task_list=task_list,
            identity=identity,
            **kwargs
        )
        token = task.get('taskToken')
        if token is None:
            raise PollTimeout("Decider poll timed out")

        events = task['events']

        next_page = task.get('nextPageToken')
        while next_page:
            try:
                task = self.connection.poll_for_decision_task(
                    self.domain.name,
                    task_list=task_list,
                    identity=identity,
                    next_page_token=next_page,
                    **kwargs
                )
            except boto.exception.SWFResponseError as e:
                message = self.get_error_message(e)
                if e.error_code == 'UnknownResourceFault':
                    raise DoesNotExistError(
                        "Unable to poll decision task",
                        message,
                    )

                raise ResponseError(message)

            token = task.get('taskToken')
            if token is None:
                raise PollTimeout("Decider poll timed out")

            events.extend(task['events'])
            next_page = task.get('nextPageToken')

        history = History.from_event_list(events)

        workflow_type = WorkflowType(
            domain=self.domain,
            name=task['workflowType']['name'],
            version=task['workflowType']['version'],
        )
        execution = WorkflowExecution(
            domain=self.domain,
            workflow_id=task['workflowExecution']['workflowId'],
            run_id=task['workflowExecution']['runId'],
            workflow_type=workflow_type,
        )

        # TODO: move history into execution (needs refactoring on WorkflowExecution.history())
        return Response(token=token, history=history, execution=execution)
