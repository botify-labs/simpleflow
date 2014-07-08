from __future__ import absolute_import

import json
import logging

import swf.models
import swf.models.decision

from simpleflow import (
    executor,
    futures,
    exceptions,
    constants,
)
from simpleflow.history import History
from simpleflow.swf.task import ActivityTask, WorkflowTask

logger = logging.getLogger(__name__)


__all__ = ['Executor']


class Executor(executor.Executor):
    def __init__(self, domain, workflow):
        super(Executor, self).__init__(workflow)
        self.domain = domain

    def _get_future_from_activity_event(self, event):
        """Maps an activity event to a Future with the corresponding state.

        :param event: workflow event.
        :type  event: swf.event.Event.

        """
        future = futures.Future()
        state = event['state']

        if state == 'scheduled':
            future._state = futures.PENDING
        elif state == 'started':
            future._state = futures.RUNNING
        elif state == 'completed':
            future._state = futures.FINISHED
            future._result = json.loads(event['result'])
        elif state == 'canceled':
            future._state = futures.CANCELLED
        elif state == 'failed':
            future._state = futures.FINISHED
            future._exception = exceptions.TaskFailed(
                reason=event['reason'],
                details=event['details'])
        elif state == 'timed_out':
            future._state = futures.FINISHED
            future._exception = exceptions.TimeoutError(
                event['timeout_type'],
                event['timeout_value'])

        return future

    def _get_future_from_child_workflow_event(self, event):
        """Maps a child workflow event to a Future with the corresponding
        state.

        """
        future = futures.Future()
        state = event['state']

        if state == 'start_initiated':
            future._state = futures.PENDING
        elif state == 'started':
            future._state = futures.RUNNING
        elif state == 'completed':
            future._state = futures.FINISHED
            future._result = json.loads(event['result'])

        return future

    def resume(self, task, *args, **kwargs):
        """Resume the execution of a task.

        If the task was scheduled, returns a future that wraps its state,
        otherwise schedules it.

        """
        task.id = self.make_task_id(task)
        event = self.find_event(task, self._history)

        if event:
            if event['type'] == 'activity':
                future = self._get_future_from_activity_event(event)
                if future.exception is None:
                    return future
                elif event.get('retry', 0) == task.activity.retry:
                    if task.activity.raises_on_failure:
                        raise exceptions.TaskException(future.exception)
                    return future
                # Otherwise retry the task by scheduling it again.
            elif event['type'] == 'child_workflow':
                future = self._get_future_from_child_workflow_event(event)
                return future

        decisions = task.schedule(self.domain)
        # ``decisions`` contains a single decision.
        self._decisions.extend(decisions)
        if len(self._decisions) == constants.MAX_DECISIONS - 1:
            # We add a timer to wake up the workflow immediately after
            # completing these decisions.
            timer = swf.models.decision.TimerDecision(
                'start',
                id='resume-after-{}'.format(task.id),
                start_to_fire_timeout='0')
            self._decisions.append(timer)
            raise exceptions.ExecutionBlocked()

        return futures.Future()  # return a pending future.

    def make_activity_task(self, func, *args, **kwargs):
        return ActivityTask(func, *args, **kwargs)

    def make_workflow_task(self, func, *args, **kwargs):
        return WorkflowTask(func, *args, **kwargs)

    def replay(self, history):
        """Executes the workflow from the start until it blocks.

        """
        self.reset()

        self._history = History(history)
        self._history.parse()

        workflow_started_event = history[0]
        args = ()
        kwargs = {}
        input = workflow_started_event.input
        if input is None:
            input = {}
        args = input.get('args', ())
        kwargs = input.get('kwargs', {})

        try:
            result = self.run_workflow(*args, **kwargs)
        except exceptions.ExecutionBlocked:
            return self._decisions, {}
        except exceptions.TaskException, err:
            reason = 'Workflow execution error: "{}"'.format(
                err.exception.reason)
            logger.exception(reason)

            details = err.exception.details
            self.on_failure(reason, details)

            decision = swf.models.decision.WorkflowExecutionDecision()
            decision.fail(
                reason=reason,
                details=details)
            return [decision], {}

        except Exception, err:
            reason = 'Cannot replay the workflow "{}"'.format(err)
            logger.exception(reason)

            self.on_failure(reason)

            decision = swf.models.decision.WorkflowExecutionDecision()
            decision.fail(reason=reason)

            return [decision], {}

        decision = swf.models.decision.WorkflowExecutionDecision()
        decision.complete(result=json.dumps(result))

        return [decision], {}

    def on_failure(self, reason, details=None):
        try:
            self._workflow.on_failure(self._history, reason, details)
        except NotImplementedError:
            pass

    def fail(self, reason, details=None):
        self.on_failure(reason, details)

        decision = swf.models.decision.WorkflowExecutionDecision()
        decision.fail(
            reason='Workflow execution failed: {}'.format(reason),
            details=details)

        self._decisions.append(decision)
        raise exceptions.ExecutionBlocked('workflow execution failed')
