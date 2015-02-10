from __future__ import absolute_import

import json
import logging

import swf.format
import swf.models
import swf.models.decision
import swf.exceptions

from simpleflow import (
    executor,
    futures,
    exceptions,
    constants,
)
from simpleflow.activity import Activity
from simpleflow.workflow import Workflow
from simpleflow.history import History
from simpleflow.swf.task import ActivityTask, WorkflowTask

logger = logging.getLogger(__name__)


__all__ = ['Executor']


class TaskRegistry(dict):
    """This registry tracks tasks and assign them an integer identifier.

    """
    def add(self, task):
        """
        ID's are assigned sequentially by incrementing an integer. They start
        from 0.

        :returns:
            :rtype: int.

        """
        name = task.name
        self[name] = self.setdefault(name, 0) + 1

        return self[name]


class Executor(executor.Executor):
    """
    Manage a workflow's execution with Amazon SWF. It replays the workflow's
    definition from the start until it blocks (i.e. raises
    :py:class:`exceptions.ExecutionBlocked`).

    SWF stores the history of all events that occurred in the workflow and
    passes it to the executor. Only one executor handles a workflow at a time.
    It means the history is consistent and there is no concurrent modifications
    on the execution of the workflow.

    """
    def __init__(self, domain, workflow):
        super(Executor, self).__init__(workflow)
        self._tasks = TaskRegistry()
        self.domain = domain

    def reset(self):
        """
        Clears the state of the execution.

        It is required to ensure the id of the tasks are assigned the same way
        on each replay.

        """
        self._decisions = []
        self._tasks = TaskRegistry()

    def _make_task_id(self, task):
        """
        Assign a new ID to *task*.

        :returns:
            String with at most 256 characters.

        """
        index = self._tasks.add(task)
        task_id = '{name}-{idx}'.format(name=task.name, idx=index)

        return task_id

    def _get_future_from_activity_event(self, event):
        """Maps an activity event to a Future with the corresponding state.

        :param event: workflow event.
        :type  event: swf.event.Event.

        """
        future = futures.Future()
        state = event['state']

        if state == 'scheduled':
            future._state = futures.PENDING
        elif state == 'schedule_failed':
            if event['cause'] == 'ACTIVITY_TYPE_DOES_NOT_EXIST':
                activity_type = swf.models.ActivityType(
                    self.domain,
                    name=event['activity_type']['name'],
                    version=event['activity_type']['version'])
                logger.info('Creating activity type {} in domain {}'.format(
                    activity_type.name,
                    self.domain.name))
                try:
                    activity_type.save()
                except swf.exceptions.AlreadyExistsError:
                    logger.info(
                        'Activity type {} in domain {} already exists'.format(
                            activity_type.name,
                            self.domain.name))
                return None
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

    def find_activity_event(self, task, history):
        activity = history._activities.get(task.id)
        return activity

    def find_child_workflow_event(self, task, history):
        return history._child_workflows.get(task.id)

    def find_event(self, task, history):
        if isinstance(task, ActivityTask):
            return self.find_activity_event(task, history)
        elif isinstance(task, WorkflowTask):
            return self.find_child_workflow_event(task, history)
        else:
            return TypeError('invalid type {} for task {}'.format(
                type(task), task))

        return None

    def make_activity_task(self, func, *args, **kwargs):
        return ActivityTask(func, *args, **kwargs)

    def make_workflow_task(self, func, *args, **kwargs):
        return WorkflowTask(func, *args, **kwargs)

    def resume_activity(self, task, event):
        future = self._get_future_from_activity_event(event)
        if not future:  # Task in history does not count.
            return None

        if not future.finished:  # Still pending or running...
            return future

        if future.exception is None:  # Result available!
            return future

        if event.get('retry', 0) == task.activity.retry:  # No more retry!
            if task.activity.raises_on_failure:
                raise exceptions.TaskException(task, future.exception)
            return future  # with future.exception set.

        # Otherwise retry the task by scheduling it again.
        return None  # means the is not in SWF.

    def resume_child_workflow(self, task, event):
        return self._get_future_from_child_workflow_event(event)

    def schedule_task(self, task):
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

    def resume(self, task, *args, **kwargs):
        """Resume the execution of a task.

        If the task was scheduled, returns a future that wraps its state,
        otherwise schedules it.

        """
        task.id = self._make_task_id(task)
        event = self.find_event(task, self._history)

        future = None
        if event:
            if event['type'] == 'activity':
                future = self.resume_activity(task, event)
            elif event['type'] == 'child_workflow':
                future = self.resume_child_workflow(task, event)

        if not future:
            self.schedule_task(task)
            future = futures.Future()  # return a pending future.

        return future

    def submit(self, func, *args, **kwargs):
        """Register a function and its arguments for asynchronous execution.

        ``*args`` and ``**kwargs`` must be serializable in JSON.

        """
        try:
            args = [executor.get_actual_value(arg) for arg in args]
            kwargs = {key: executor.get_actual_value(val) for
                      key, val in kwargs.iteritems()}
        except exceptions.ExecutionBlocked:
            return futures.Future()

        try:
            if isinstance(func, Activity):
                task = self.make_activity_task(func, *args, **kwargs)
            elif issubclass(func, Workflow):
                task = self.make_workflow_task(func, *args, **kwargs)
            else:
                raise TypeError
        except TypeError:
            raise TypeError('invalid type {} for {}'.format(
                type(func), func))

        return self.resume(task, *args, **kwargs)

    def map(self, callable, iterable):
        """Submit *callable* with each of the items in ``*iterables``.

        All items in ``*iterables`` must be serializable in JSON.

        """
        iterable = executor.get_actual_value(iterable)
        return super(Executor, self).map(callable, iterable)

    def starmap(self, callable, iterable):
        iterable = executor.get_actual_value(iterable)
        return super(Executor, self).starmap(callable, iterable)

    def merge_previous_execution(self, execution):
        previous_history = History(execution.history())
        # Override input with the previous execution value.
        self._history.events[0].input = json.dumps(
            previous_history.events[0].input.copy(),
        )

        # Override already completed tasks to not execute them again.
        previous_history.parse()
        self._history._activities.update({
            id_: activity for id_, activity in
            previous_history._activities.iteritems() if
            activity['state'] == 'completed'
        })
        self._history._child_workflows.update({
            id_: child_workflow for id_, child_workflow in
            previous_history._child_workflows.iteritems() if
            child_workflow['state'] == 'completed'
        })

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

        previous_workflow_execution = input.get('_previous_workflow_execution')
        if previous_workflow_execution:
            # Resume previous execution by injecting input and completed task
            # in the current history.
            ex = swf.models.WorkflowExecution(
                domain=self.domain,
                workflow_id=previous_workflow_execution['workflow_id'],
                run_id=previous_workflow_execution['run_id'],
            )
            self.merge_previous_execution(ex)

        try:
            result = self.run_workflow(*args, **kwargs)
        except exceptions.ExecutionBlocked:
            return self._decisions, {}
        except exceptions.TaskException, err:
            reason = 'Workflow execution error in task {}: "{}"'.format(
                err.task.name,
                getattr(err.exception, 'reason', repr(err.exception)))
            logger.exception(reason)

            details = getattr(err.exception, 'details', None)
            self.on_failure(reason, details)

            decision = swf.models.decision.WorkflowExecutionDecision()
            decision.fail(
                reason=swf.format.reason(reason),
                details=swf.format.details(details),
            )
            return [decision], {}

        except Exception, err:
            reason = 'Cannot replay the workflow {}({})'.format(
                err.__class__.__name__,
                err)
            logger.exception(reason)

            self.on_failure(reason)

            decision = swf.models.decision.WorkflowExecutionDecision()
            decision.fail(reason=swf.format.reason(reason))

            return [decision], {}

        decision = swf.models.decision.WorkflowExecutionDecision()
        decision.complete(result=swf.format.result(json.dumps(result)))

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
            reason=swf.format.reason(
                'Workflow execution failed: {}'.format(reason)),
            details=swf.format.details(details),
        )

        self._decisions.append(decision)
        raise exceptions.ExecutionBlocked('workflow execution failed')

    def run(self, history):
        return self.replay(history)
