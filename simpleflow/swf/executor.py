from __future__ import absolute_import

import hashlib
import json
import logging
import traceback

import swf.format
import swf.models
import swf.models.decision
import swf.exceptions

from simpleflow import (
    exceptions,
    executor,
    futures,
    task,
)
from simpleflow.activity import Activity
from simpleflow.workflow import Workflow
from simpleflow.history import History
from simpleflow.swf.task import ActivityTask, WorkflowTask
from simpleflow.swf import constants

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
    def __init__(self, domain, workflow, task_list=None):
        super(Executor, self).__init__(workflow)
        self._tasks = TaskRegistry()
        self.domain = domain
        self.task_list = task_list

    def reset(self):
        """
        Clears the state of the execution.

        It is required to ensure the id of the tasks are assigned the same way
        on each replay.

        """
        self._open_activity_count = 0
        self._decisions = []
        self._tasks = TaskRegistry()

    def _make_task_id(self, task, *args, **kwargs):
        """
        Assign a new ID to *task*.

        :returns:
            String with at most 256 characters.

        """
        if not task.idempotent:
            # If idempotency is False or unknown, let's generate a task id by
            # incrementing and id after the task name.
            # (default strategy, backwards compatible with previous versions)
            suffix = self._tasks.add(task)
        else:
            # If task is idempotent, we can do better and hash arguments.
            # It makes the workflow resistant to retries or variations on the
            # same task name (see #11).
            arguments = json.dumps({"args": args, "kwargs": kwargs})
            suffix = hashlib.md5(arguments).hexdigest()

        task_id = '{name}-{idx}'.format(name=task.name, idx=suffix)
        return task_id

    def _get_future_from_activity_event(self, event):
        """Maps an activity event to a Future with the corresponding state.

        :param event: workflow event.
        :type  event: swf.event.Event.

        """
        future = futures.Future()  # state is PENDING.
        state = event['state']

        if state == 'scheduled':
            future._state = futures.PENDING
        elif state == 'schedule_failed':
            if event['cause'] == 'ACTIVITY_TYPE_DOES_NOT_EXIST':
                activity_type = swf.models.ActivityType(
                    self.domain,
                    name=event['activity_type']['name'],
                    version=event['activity_type']['version'])
                logger.info('creating activity type {} in domain {}'.format(
                    activity_type.name,
                    self.domain.name))
                try:
                    activity_type.save()
                except swf.exceptions.AlreadyExistsError:
                    logger.info(
                        'oops: Activity type {} in domain {} already exists, creation failed, continuing...'.format(
                            activity_type.name,
                            self.domain.name))
                return None
            logger.info('failed to schedule {}: {}'.format(
                event['activity_type']['name'],
                event['cause'],
            ))
            return None
        elif state == 'started':
            future._state = futures.RUNNING
        elif state == 'completed':
            future._state = futures.FINISHED
            result = event['result']
            future._result = json.loads(result) if result else None
        elif state == 'canceled':
            future._state = futures.CANCELLED
        elif state == 'failed':
            future._state = futures.FINISHED
            future._exception = exceptions.TaskFailed(
                name=event['id'],
                reason=event['reason'],
                details=event.get('details'))
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
            raise TypeError('invalid type {} for task {}'.format(
                type(task), task))

    def resume_activity(self, task, event):
        future = self._get_future_from_activity_event(event)
        if not future:  # Task in history does not count.
            return None

        if not future.finished:  # Still pending or running...
            return future

        if future.exception is None:  # Result available!
            return future

        # Compare number of retries in history with configured max retries
        # NB: we used to do a strict comparison (==), but that can lead to
        # inifinite retries in case the code is redeployed with a decreased
        # retry limit and a workflow has a already crossed the new limit. So
        # ">=" is better there.
        if event.get('retry', 0) >= task.activity.retry:
            if task.activity.raises_on_failure:
                raise exceptions.TaskException(task, future.exception)
            return future  # with future.exception set.

        # Otherwise retry the task by scheduling it again.
        return None  # means the is not in SWF.

    def resume_child_workflow(self, task, event):
        return self._get_future_from_child_workflow_event(event)

    def schedule_task(self, task, task_list=None):
        logger.debug('executor is scheduling task {} on task_list {}'.format(
            task.name,
            task_list,
        ))
        decisions = task.schedule(self.domain, task_list)
        # ``decisions`` contains a single decision.
        self._decisions.extend(decisions)
        self._open_activity_count += 1
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
        task.id = self._make_task_id(task, *args, **kwargs)
        event = self.find_event(task, self._history)

        future = None
        if event:
            if event['type'] == 'activity':
                future = self.resume_activity(task, event)
                if future and future._state in (futures.PENDING, futures.RUNNING):
                    self._open_activity_count += 1
            elif event['type'] == 'child_workflow':
                future = self.resume_child_workflow(task, event)

        if not future:
            self.schedule_task(task, task_list=self.task_list)
            future = futures.Future()  # return a pending future.

        if self._open_activity_count == constants.MAX_OPEN_ACTIVITY_COUNT:
            logger.warning('limit of {} open activities reached'.format(
                constants.MAX_OPEN_ACTIVITY_COUNT))
            raise exceptions.ExecutionBlocked

        return future

    def submit(self, func, *args, **kwargs):
        """Register a function and its arguments for asynchronous execution.

        ``*args`` and ``**kwargs`` must be serializable in JSON.

        """
        try:
            if isinstance(func, Activity):
                task = ActivityTask(func, *args, **kwargs)
            elif issubclass(func, Workflow):
                task = WorkflowTask(func, *args, **kwargs)
            else:
                # NB: isinstance() and issubclass() may raise a TypeError too
                # hence the try/except reraising a TypeError. Found reason in
                # commit 8faa8636.
                # TODO: see if we can avoid that, that hides TypeError's in
                # tasks creation, which is annoying, because the re-raised
                # exception can be misleading in that case.
                raise TypeError
        except exceptions.ExecutionBlocked:
            return futures.Future()
        except TypeError:
            raise TypeError('invalid type {} for {}'.format(
                type(func), func))

        return self.resume(task, *task.args, **task.kwargs)

    # TODO: check if really used or remove it
    def map(self, callable, iterable):
        """Submit *callable* with each of the items in ``*iterables``.

        All items in ``*iterables`` must be serializable in JSON.

        """
        iterable = task.get_actual_value(iterable)
        return super(Executor, self).map(callable, iterable)

    # TODO: check if really used or remove it
    def starmap(self, callable, iterable):
        iterable = task.get_actual_value(iterable)
        return super(Executor, self).starmap(callable, iterable)

    def replay(self, decision_response):
        """Executes the workflow from the start until it blocks.

        :param decision_response: an object wrapping the PollForDecisionTask response
        :type  decision_response: swf.responses.Response

        :returns: a list of decision and a context dict
        :rtype: ([swf.models.decision.base.Decision], dict)
        """
        self.reset()

        history = decision_response.history
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

        self.before_replay()
        try:
            result = self.run_workflow(*args, **kwargs)
        except exceptions.ExecutionBlocked:
            logger.info('{} open activities ({} decisions)'.format(
                self._open_activity_count,
                len(self._decisions),
            ))
            self.after_replay()
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
            self.after_closed()
            return [decision], {}

        except Exception, err:
            reason = 'Cannot replay the workflow: {}({})'.format(
                err.__class__.__name__,
                err,
            )

            tb = traceback.format_exc()
            details = 'Traceback:\n{}'.format(tb)
            logger.exception(reason + '\n' + details)

            self.on_failure(reason)

            decision = swf.models.decision.WorkflowExecutionDecision()
            decision.fail(
                reason=swf.format.reason(reason),
                details=swf.format.details(details),
            )
            self.after_closed()
            return [decision], {}

        self.after_replay()
        decision = swf.models.decision.WorkflowExecutionDecision()
        decision.complete(result=swf.format.result(json.dumps(result)))
        self.on_completed()
        self.after_closed()
        return [decision], {}

    def before_replay(self):
        return self._workflow.before_replay(self._history)

    def after_replay(self):
        return self._workflow.after_replay(self._history)

    def after_closed(self):
        return self._workflow.after_closed(self._history)

    def on_failure(self, reason, details=None):
        try:
            self._workflow.on_failure(self._history, reason, details)
        except NotImplementedError:
            pass

    def on_completed(self):
        try:
            self._workflow.on_completed(self._history)
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

    def run(self, decision_response):
        return self.replay(decision_response)
