from __future__ import absolute_import

import copy
import inspect
import hashlib
import json
import multiprocessing
import re
import traceback

import simpleflow.task as base_task
import swf.exceptions
import swf.models
import swf.models.decision
from simpleflow import (
    exceptions,
    executor,
    format,
    futures,
    logger,
    task,
    compat,
)
from simpleflow.activity import Activity, PRIORITY_NOT_SET
from simpleflow.base import Submittable
from simpleflow.history import History
from simpleflow.marker import Marker
from simpleflow.signal import WaitForSignal
from simpleflow.swf import constants
from simpleflow.swf.helpers import swf_identity
from simpleflow.swf.utils import DecisionsAndContext
from simpleflow.swf.task import (
    SwfTask,
    ActivityTask,
    WorkflowTask,
    SignalTask,
    MarkerTask,
    TimerTask,
    CancelTimerTask,
)
from simpleflow.utils import (
    hex_hash,
    issubclass_,
    json_dumps,
    retry,
)
from simpleflow.workflow import Workflow
from swf.core import ConnectedSWFObject

# noinspection PyUnreachableCode
if False:
    from typing import Optional, Type, Union, Tuple  # NOQA

__all__ = ['Executor']


# if "poll_for_activity_task" doesn't contain a "taskToken"
# key, then retry ; it happens (not often) that the decider
# doesn't get the scheduled task while it should...
@retry.with_delay(nb_times=3,
                  delay=retry.exponential,
                  on_exceptions=KeyError)
def run_fake_activity_task(domain, task_list, result):
    conn = ConnectedSWFObject().connection
    resp = conn.poll_for_activity_task(
        domain,
        task_list,
        identity=swf_identity(),
    )
    conn.respond_activity_task_completed(
        resp['taskToken'],
        result,
    )


# Same retry condition as run_fake_activity_task
@retry.with_delay(nb_times=3,
                  delay=retry.exponential,
                  on_exceptions=KeyError)
def run_fake_child_workflow_task(domain, task_list, result=None):
    conn = ConnectedSWFObject().connection
    resp = conn.poll_for_decision_task(
        domain,
        task_list,
        identity=swf_identity(),
    )
    conn.respond_decision_task_completed(
        resp['taskToken'],
        decisions=[
            {
                'decisionType': 'CompleteWorkflowExecution',
                'completeWorkflowExecutionDecisionAttributes': {
                    'result': result,
                },
            }
        ]
    )


def run_fake_task_worker(domain, task_list, former_event):
    if former_event['type'] == 'activity':
        worker_proc = multiprocessing.Process(
            target=run_fake_activity_task,
            args=(
                domain,
                task_list,
                former_event['result'],
            ),
        )
    elif former_event['type'] == 'child_workflow':
        worker_proc = multiprocessing.Process(
            target=run_fake_child_workflow_task,
            args=(
                domain,
                task_list,
            ),
            kwargs={
                'result': former_event['result'],
            },
        )
    else:
        raise Exception('Wrong event type {}'.format(former_event['type']))

    worker_proc.start()


class TaskRegistry(dict):
    """This registry tracks tasks and assign them an integer identifier.

    """

    def add(self, a_task):
        """
        ID's are assigned sequentially by incrementing an integer. They start
        from 1.

        :type a_task: ActivityTask | WorkflowTask
        :returns:
            :rtype: int.

        """
        name = a_task.name
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

    :ivar domain: domain
    :type domain: swf.models.domain.Domain
    :ivar task_list: task list
    :type task_list: Optional[str]
    :ivar repair_with: previous history to use for repairing
    :type repair_with: Optional[simpleflow.history.History]
    :ivar force_activities: regex with activities to force
    :type _history: History
    :ivar _repair_workflow_id: workflow ID to repair, if any
    :type _repair_workflow_id: Optional[str]
    :ivar repair_run_id: run ID to repair, if any
    :type _repair_run_id: Optional[str]

    """

    def __init__(self, domain, workflow_class, task_list=None, repair_with=None,
                 force_activities=None,
                 repair_workflow_id=None, repair_run_id=None,
                 ):
        super(Executor, self).__init__(workflow_class)
        self._history = None  # type: Optional[History]
        self._run_context = {}
        self.domain = domain
        self.task_list = task_list
        self.repair_with = repair_with
        self._repair_workflow_id = repair_workflow_id
        self._repair_run_id = repair_run_id
        if force_activities:
            self.force_activities = re.compile(force_activities)
        else:
            self.force_activities = None
        self._open_activity_count = 0
        self._decisions_and_context = DecisionsAndContext()
        self._append_timer = False  # Append an immediate timer decision
        self._tasks = TaskRegistry()
        self._idempotent_tasks_to_submit = set()
        self._execution = None
        self.current_priority = None
        self.handled_failures = {}
        self.created_activity_types = set()

    def reset(self):
        """
        Clears the state of the execution.

        It is required to ensure the id of the tasks are assigned the same way
        on each replay.

        """
        self._open_activity_count = 0
        self._decisions_and_context = DecisionsAndContext()
        self._append_timer = False  # Append an immediate timer decision
        self._tasks = TaskRegistry()
        self._idempotent_tasks_to_submit = set()
        self._execution = None
        self.current_priority = None
        self.handled_failures = {}
        self.created_activity_types = set()
        self.create_workflow()

    def _make_task_id(self, a_task, workflow_id, run_id, *args, **kwargs):
        """
        Assign a new ID to *a_task*.

        :type a_task: ActivityTask | WorkflowTask
        :type workflow_id: str
        :type run_id: str
        :returns:
            String with at most 256 characters.
        :rtype: str

        """
        if isinstance(a_task, ActivityTask) and hasattr(a_task.activity.callable, 'get_task_id'):
            suffix = a_task.activity.callable.get_task_id(self.workflow, *args, **kwargs)
        elif not a_task.idempotent:
            # If idempotency is False or unknown, let's generate a task id by
            # incrementing an id after the a_task name.
            # (default strategy, backwards compatible with previous versions)
            suffix = self._tasks.add(a_task)
        else:
            # If a_task is idempotent, we can do better and hash arguments.
            # It makes the workflow resistant to retries or variations on the
            # same task name (see #11).
            arguments = json_dumps({"args": args, "kwargs": kwargs})
            suffix = hashlib.md5(arguments.encode('utf-8')).hexdigest()

        if isinstance(a_task, (WorkflowTask,)):
            # Some task types must have globally unique names.
            suffix = '{}--{}--{}'.format(workflow_id, hex_hash(run_id), suffix)

        task_id = '{name}-{suffix}'.format(name=a_task.name, suffix=suffix)
        if len(task_id) > 256:  # Better safe than sorry...
            task_id = task_id[0:223] + "-" + hashlib.md5(task_id.encode('utf-8')).hexdigest()

        return task_id

    def _get_future_from_activity_event(self, event):
        """Maps an activity event to a Future with the corresponding state.

        :param event: activity event
        :type  event: dict[str, Any]
        :rtype: futures.Future

        """
        future = futures.Future()  # state is PENDING.
        state = event['state']

        if state == 'scheduled':
            pass
        elif state == 'schedule_failed':
            name = event['activity_type']['name']
            version = event['activity_type']['version']
            if event['cause'] == 'ACTIVITY_TYPE_DOES_NOT_EXIST' and (name, version) not in self.created_activity_types:
                self.created_activity_types.add((name, version))
                activity_type = swf.models.ActivityType(
                    self.domain,
                    name=name,
                    version=version)
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
                name,
                event['cause'],
            ))
            return None
        elif state == 'started':
            future.set_running()
        elif state == 'completed':
            result = event['result']
            future.set_finished(format.decode(result))
        elif state == 'canceled':
            future.set_cancelled()
        elif state == 'failed':
            exception = exceptions.TaskFailed(
                name=event['id'],
                reason=event['reason'],
                details=event.get('details'))
            future.set_exception(exception)
        elif state == 'timed_out':
            exception = exceptions.TimeoutError(
                event['timeout_type'],
                event['timeout_value'])
            future.set_exception(exception)
        else:
            logger.info(
                'unhandled state for activity %s: %s',
                event.get('name', '#{}'.format(event['id'])),
                state
            )

        return future

    def _get_future_from_child_workflow_event(self, event):
        """Maps a child workflow event to a Future with the corresponding
        state.

        :param event: child workflow event
        :type  event: dict[str, Any]
        """
        future = futures.Future()
        state = event['state']

        if state == 'start_initiated':
            pass  # future._state = futures.PENDING
        elif state == 'start_failed':
            if event['cause'] == 'WORKFLOW_TYPE_DOES_NOT_EXIST':
                workflow_type = swf.models.WorkflowType(
                    self.domain,
                    name=event['name'],
                    version=event['version'],
                )
                logger.info('Creating workflow type {} in domain {}'.format(
                    workflow_type.name,
                    self.domain.name,
                ))
                try:
                    workflow_type.save()
                except swf.exceptions.AlreadyExistsError:
                    # Could have be created by a concurrent workflow execution.
                    pass
                return None
            future.set_exception(exceptions.TaskFailed(
                name=event['id'],
                reason=event['cause'],
                details=event.get('details'),
            ))
        elif state == 'started':
            future.set_running()
        elif state == 'completed':
            future.set_finished(format.decode(event['result']))
        elif state == 'failed':
            future.set_exception(exceptions.TaskFailed(
                name=event['id'],
                reason=event['reason'],
                details=event.get('details'),
            ))
        elif state == 'timed_out':
            future.set_exception(exceptions.TimeoutError(
                event['timeout_type'],
                None,
            ))
        elif state == 'canceled':
            future.set_exception(exceptions.TaskCanceled(
                event.get('details'),
            ))
        elif state == 'terminated':
            future.set_exception(exceptions.TaskTerminated())
        else:
            logger.info(
                'unhandled state for workflow %s: %s',
                event.get('name', '#{}'.format(event['id'])),
                state
            )

        return future

    def _get_future_from_marker_event(self, a_task, event):
        """Maps a marker event to a Future with the corresponding
        state.

        :param a_task: currently unused
        :type a_task:
        :param event: marker event
        :type  event: dict[str, Any]
        :rtype: futures.Future
        """
        future = futures.Future()
        if not event:
            return future
        state = event['state']
        if state == 'recorded':
            future.set_finished(event['details'])
        elif state == 'failed':
            future.set_exception(exceptions.TaskFailed(
                name=event['name'],
                reason=event['cause'],
            ))

        return future

    def get_future_from_signal_event(self, a_task, event):
        """Maps a signal event to a Future with the corresponding
        state.

        :param a_task: currently unused
        :type a_task: Optional[SignalTask]
        :param event: signal event
        :type  event: dict[str, Any]
        :rtype: futures.Future
        """
        future = futures.Future()
        if not event:
            return future
        state = event['state']
        if state == 'signaled':
            future.set_finished(event['input'])

        return future

    def get_future_from_external_workflow_event(self, a_task, event):
        """Maps an external workflow event to a Future with the corresponding
        state.

        :param a_task: currently unused
        :type a_task:
        :param event: external workflow event
        :type  event: dict[str, Any]
        :rtype: futures.Future
        """
        future = futures.Future()
        if not event:
            return future
        state = event['state']
        if state == 'signal_execution_initiated':
            # Don't re-initiate signal sending
            future.set_running()
        elif state == 'execution_signaled':
            future.set_finished(event['input'])
        elif state == 'signal_execution_failed':
            future.set_exception(exceptions.TaskFailed(
                name=event['name'],
                reason=event['cause'],
            ))

        return future

    def _get_future_from_timer_event(self, a_task, event):
        """
        Maps a timer event to a Future with the corresponding state.

        :param a_task: Timer task; unused.
        :type a_task: TimerTask
        :param event: Timer event
        :type event: dict[str, Any]
        :return:
        :rtype: futures.Future
        """
        future = futures.Future()
        if not event:
            return future
        state = event['state']
        if state == 'started':
            future.set_running()
        elif state == 'fired':
            future.set_finished(None)
        elif state == 'canceled':
            future.set_cancelled()
        elif state in ('start_failed', 'cancel_failed'):
            future.set_exception(exceptions.TaskFailed(
                name=event['timer_id'],
                reason=event['cause'],
            ))

        return future

    def get_future_from_signal(self, signal_name):
        """

        :param signal_name:
        :type signal_name: str
        :return:
        :rtype: futures.Future
        """
        event = self._history.signals.get(signal_name)
        return self.get_future_from_signal_event(None, event)

    def find_activity_event(self, a_task, history):
        """
        Get the event corresponding to an activity task, if any.

        :param a_task:
        :type a_task: ActivityTask
        :param history:
        :type history: simpleflow.history.History
        :return:
        :rtype: Optional[dict[str, Any]]
        """
        activity = history.activities.get(a_task.id)
        return activity

    def find_child_workflow_event(self, a_task, history):
        """
        Get the event corresponding to a child workflow, if any.

        :param a_task:
        :type a_task: WorkflowTask
        :param history:
        :type history: simpleflow.history.History
        :return:
        :rtype: Optional[dict]
        """
        return history.child_workflows.get(a_task.id)

    def find_signal_event(self, a_task, history):
        """
        Get the event corresponding to a signal, if any.

        :param a_task:
        :type a_task: SignalTask
        :param history:
        :type history: simpleflow.history.History
        :return:
        :rtype: Optional[dict]
        """
        # FIXME could look directly in signaled_workflows?
        event = history.signals.get(a_task.name)
        if not event:
            if a_task.workflow_id is None:  # Broadcast, should be in signals
                return None
            signaled_workflows = history.signaled_workflows.get(a_task.name, [])
            for w in signaled_workflows:
                if w['workflow_id'] == a_task.workflow_id and (a_task.run_id is None or w['run_id'] == a_task.run_id):
                    event = w
                    break
        return event

    def find_marker_event(self, a_task, history):
        """
        Get the event corresponding to a marker, if any.

        :param a_task:
        :type a_task: MarkerTask
        :param history:
        :type history: simpleflow.history.History
        :return:
        :rtype: Optional[dict[str, Any]]
        """
        json_details = json_dumps(a_task.details) if a_task.details is not None else None
        marker_list = history.markers.get(a_task.name)
        if not marker_list:
            return None
        marker_list = list(
            filter(
                lambda m: m['state'] == 'recorded' and m['details'] == json_details,
                marker_list
            )
        )
        return marker_list[-1] if marker_list else None

    def find_timer_event(self, a_task, history):
        """
        Get the event corresponding to a timer or timer cancellation, if any.

        :param a_task:
        :type a_task: TimerTask | CancelTimerTask
        :param history:
        :type history: simpleflow.history.History
        :return:
        :rtype: Optional[dict[str, Any]]
        """
        event = history.timers.get(a_task.id)
        if not event:
            return None
        if isinstance(a_task, CancelTimerTask):
            if 'canceled_event_id' not in event and 'cancel_failed_event_id' not in event:
                # Timer not yet cancelled: no future returned
                return None
        return event

    TASK_TYPE_TO_EVENT_FINDER = {
        ActivityTask: find_activity_event,
        WorkflowTask: find_child_workflow_event,
        SignalTask: find_signal_event,
        MarkerTask: find_marker_event,
        TimerTask: find_timer_event,
        CancelTimerTask: find_timer_event,
    }

    def find_event(self, a_task, history):
        """
        Get the event corresponding to a "task", if any.
        :param a_task:
        :type a_task: SwfTask
        :param history:
        :type history: simpleflow.history.History
        :return:
        :rtype: Optional[dict]
        """
        for typ in inspect.getmro(type(a_task)):
            finder = self.TASK_TYPE_TO_EVENT_FINDER.get(typ)
            if finder:
                return finder(self, a_task, history)
        raise TypeError('invalid type {} for task {}'.format(
            type(a_task), a_task))

    def resume_activity(self, a_task, event):
        """
        Resume an activity task.
        :param a_task:
        :type a_task: ActivityTask
        :param event:
        :type event: dict
        :return:
        :rtype: Optional[futures.Future]
        """
        future = self._get_future_from_activity_event(event)
        if not future:  # schedule failed, maybe OK later.
            return None

        if not future.finished:  # Still pending or running...
            return future

        if future.exception is None:  # Result available!
            return future

        return self.handle_failure(event, future, a_task, exceptions.TaskException)

    def resume_child_workflow(self, a_workflow, event):
        """
        Resume a child workflow.

        :param a_workflow:
        :type a_workflow: WorkflowTask
        :param event:
        :type event: dict
        :return:
        :rtype: Optional[simpleflow.futures.Future]
        """
        future = self._get_future_from_child_workflow_event(event)

        if not future:
            # WORKFLOW_TYPE_DOES_NOT_EXIST, will be created
            return None

        if not future.finished:  # Still pending or running...
            return future

        if future.exception is None:  # Result available!
            return future

        return self.handle_failure(event, future, a_workflow, exceptions.WorkflowException)

    def handle_failure(self,
                       event,  # type: dict
                       future,  # type: futures.Future
                       swf_task,  # type: Union[ActivityTask, WorkflowTask]
                       exception_class,  # type: Type[Exception]
                       ):
        # type: (...) -> Union[futures.Future, Tuple[Optional[futures.Future], SwfTask], None]
        """
        Call the workflow's on_task_failure method if it exists.
        If no retry/abort/ignore decision, use the default strategy (using retry count and raises_on_failure).

        on_task_failure can:
        * abort the task
        * ignore the error, and set the future's result as wanted
        * cancel the task (the future will be marked "cancelled")
        * retry as many times as wanted, immediately or with a wait period
        * do nothing: the default error handling is used

        :param event:
        :param future:
        :param swf_task:
        :param exception_class:
        :return:
        """
        event_id = History.get_event_id(event)
        if event_id in self.handled_failures:  # don't call workflow method multiple times
            return self.handled_failures[event_id]

        logger.debug('handle_failure: failed_id=%s', event_id)
        rc = self.do_handle_failure(event, future, swf_task, exception_class)
        self.handled_failures[event_id] = rc
        return rc

    def do_handle_failure(self,
                          event,  # type: dict
                          future,  # type: futures.Future
                          swf_task,  # type: Union[ActivityTask, WorkflowTask]
                          exception_class,  # type: Type[Exception]
                          ):
        # type: (...) -> Union[futures.Future, Tuple[Optional[futures.Future], SwfTask], None]
        timer = self.find_timer_associated_with(event, swf_task)
        if timer:
            if isinstance(timer['control'], compat.string_types):  # FIXME unconditional?
                control = format.decode(timer['control'])
            else:
                control = timer['control']
            if not isinstance(control, dict):
                control = {}
            if timer['state'] == 'started':
                logger.debug('handle_failure: timer {} started, "pending" future'.format(timer['id']))
                return futures.Future(), swf_task  # mark as pending
            elif timer['state'] in ('fired', 'canceled'):
                logger.debug('handle_failure: timer {} fired or canceled, retrying'.format(timer['id']))
                swf_task.args = control.get('args', ())
                swf_task.kwargs = control.get('kwargs', {})
                return None, swf_task
            elif timer['state'] == 'start_failed':
                raise exceptions.TaskFailed('timer', timer['id'], timer['cause'])
            else:  # TODO: handle
                logger.warning('Unexpected timer state for timer "{}": {}'.format(timer['id'], timer['state']))

        failure_context = base_task.TaskFailureContext(swf_task, event, future, exception_class, self._history)
        if hasattr(self.workflow, 'on_task_failure'):
            new_failure_context = self.workflow.on_task_failure(failure_context)  # type: base_task.TaskFailureContext
            if new_failure_context:
                failure_context = new_failure_context
            future, swf_task, event = failure_context.future, failure_context.a_task, failure_context.event  # updatable
            if failure_context.decision == base_task.TaskFailureContext.Decision.abort:
                if swf_task.payload.raises_on_failure:
                    raise exception_class(swf_task, future.exception)
                return future, swf_task
            elif failure_context.decision == base_task.TaskFailureContext.Decision.ignore:
                future.set_exception(None)
                return future, swf_task
            elif failure_context.decision == base_task.TaskFailureContext.Decision.cancel:
                future.set_cancelled()
                return future, swf_task
            elif (failure_context.decision == base_task.TaskFailureContext.Decision.retry_now or
                    (failure_context.decision == base_task.TaskFailureContext.Decision.retry_later and
                     not failure_context.retry_wait_timeout)):
                return None, swf_task
            elif failure_context.decision == base_task.TaskFailureContext.Decision.retry_later:
                return (
                    None,
                    TimerTask(
                        self.get_retry_task_timer_id(swf_task),
                        failure_context.retry_wait_timeout,
                        swf_task.get_input()
                    ),
                )
            elif failure_context.decision == base_task.TaskFailureContext.Decision.handled:
                return future, swf_task
            if failure_context.decision != base_task.TaskFailureContext.Decision.none:
                raise ValueError('Unexpected TaskFailureValue decision: {}'.format(failure_context.decision))

        new_failure_context = self.default_failure_handling(failure_context)
        return new_failure_context.future

    @staticmethod
    def default_failure_handling(failure_context):
        # type: (base_task.TaskFailureContext) -> base_task.TaskFailureContext

        # Compare number of retries in history with configured max retries
        # NB: we used to do a strict comparison (==), but that can lead to
        # infinite retries in case the code is redeployed with a decreased
        # retry limit and a workflow has a already crossed the new limit. So
        # ">=" is better there.
        if failure_context.event.get('retry', 0) >= failure_context.a_task.payload.retry:
            if failure_context.a_task.payload.raises_on_failure:
                raise failure_context.exception_class(failure_context.a_task, failure_context.future.exception)
        else:
            # Otherwise retry the workflow by scheduling it again.
            failure_context.future = None  # means it is not in SWF.
        failure_context.decision = base_task.TaskFailureContext.Decision.handled
        return failure_context

    def find_timer_associated_with(self, event, swf_task):
        # type: (dict, Union[ActivityTask, WorkflowTask]) -> Optional[dict]
        """
        Return a potential timer "associated with" an event, i.e.
        * with a related name
        * launched in a decision completed after the event's decision
        :param event:
        :param swf_task:
        :return:
        """
        timer = self._history.timers.get(self.get_retry_task_timer_id(swf_task))
        if timer and timer['decision_task_completed_event_id'] > event['decision_task_completed_event_id']:
            return timer
        return None

    @staticmethod
    def get_retry_task_timer_id(swf_task):
        return '__simpleflow_task_{}'.format(str(swf_task.id))

    def schedule_task(self, a_task, task_list=None):
        """
        Let a task schedule itself.
        If too many decisions are in flight, add a timer decision and raise ExecutionBlocked.
        :param a_task:
        :type a_task: ActivityTask | WorkflowTask | SignalTask | MarkerTask
        :param task_list:
        :type task_list: Optional[str]
        :raise: exceptions.ExecutionBlocked if too many decisions waiting
        """

        if a_task.idempotent:
            task_identifier = (type(a_task), self.domain, a_task.id)
            if task_identifier in self._idempotent_tasks_to_submit:
                logger.debug('Not resubmitting task {}'.format(a_task.name))
                return
            self._idempotent_tasks_to_submit.add(task_identifier)

        # NB: ``decisions`` contains a single decision.
        decisions = a_task.schedule(self.domain, task_list, priority=self.current_priority, executor=self)

        # Ready to schedule
        if isinstance(a_task, ActivityTask):
            self._open_activity_count += 1
        elif isinstance(a_task, (MarkerTask, CancelTimerTask)):
            self._append_timer = True  # Marker and CancelTimer don't generate decisions, so force a wake-up timer

        # Check if we won't violate the 1MB limit on API requests ; if so, do NOT
        # schedule the requested task and block execution instead, with a timer
        # to wake up the workflow immediately after completing these decisions.
        # See: http://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dg-limits.html
        # NB: here we use json.dumps, not json_dumps, since the serialization will
        # happen inside boto.swf and is out of our control.
        request_size = len(json.dumps(self._decisions_and_context.decisions + decisions))
        # We keep a 5kB of error margin for headers, json structure, and the
        # timer decision, and 32kB for the context, even if we don't use it now.
        if request_size > constants.MAX_REQUEST_SIZE - 5000 - 32000:
            # TODO: at this point we may check that self._decisions is not empty
            # If it's the case, it means that a single decision was weighting
            # more than 900kB, so we have bigger problems.
            self._append_timer = True
            raise exceptions.ExecutionBlocked()

        self._decisions_and_context.extend_decision(decisions)

        # Check if we won't exceed max decisions -1
        # TODO: if we had exactly MAX_DECISIONS - 1 to take, this will wake up
        # the workflow for no reason. Evaluate if we can do better.
        if len(self._decisions_and_context.decisions) == constants.MAX_DECISIONS - 1:
            # We add a timer to wake up the workflow immediately after
            # completing these decisions.
            self._append_timer = True
            raise exceptions.ExecutionBlocked()

    def _add_start_timer_decision(self, id, timeout=0):
        timer = swf.models.decision.TimerDecision(
            'start',
            id=id,
            start_to_fire_timeout=str(timeout))
        self._decisions_and_context.append_decision(timer)

    EVENT_TYPE_TO_FUTURE = {
        'activity': resume_activity,
        'child_workflow': resume_child_workflow,
        'signal': get_future_from_signal_event,
        'external_workflow': get_future_from_external_workflow_event,
        'marker': _get_future_from_marker_event,
        'timer': _get_future_from_timer_event,
    }

    def resume(self, a_task, *args, **kwargs):
        """Resume the execution of a task.
        Called by `submit`.

        If the task was scheduled, returns a future that wraps its state,
        otherwise schedules it.
        If in repair mode, we may fake the task to repair from the previous history.

        :param a_task:
        :type a_task: Task
        :param args:
        :type args: tuple
        :type kwargs:
        :type kwargs: dict
        :rtype: futures.Future
        :raise: exceptions.ExecutionBlocked if open activities limit reached
        """
        is_repair = bool(self.repair_with)

        if not a_task.id:  # Can be already set (WorkflowTask)
            if is_repair:
                workflow_id, run_id = self._repair_workflow_id, self._repair_run_id
            else:
                workflow_id, run_id = self._workflow_id, self._run_id
            a_task.id = self._make_task_id(a_task, workflow_id, run_id, *args, **kwargs)
        event = self.find_event(a_task, self._history)
        logger.debug('executor: resume {}, event={}'.format(a_task, event))
        future = None

        # in repair mode, check if we absolutely want to re-execute this task
        force_execution = (self.force_activities and
                           self.force_activities.search(a_task.id))

        # try to fill in the blanks with the workflow we're trying to repair if any
        if not event and is_repair and not force_execution:
            # try to find a former event matching this task
            former_event = self.find_event(a_task, self.repair_with)
            # ... but only keep the event if the task was successful
            if former_event and former_event['state'] == 'completed':
                logger.info(
                    'faking task completed successfully in previous '
                    'workflow: {}'.format(former_event['id'])
                )
                json_hash = hashlib.md5(json_dumps(former_event).encode('utf-8')).hexdigest()
                fake_task_list = "FAKE-" + json_hash

                # schedule task on a fake task list
                self.schedule_task(a_task, task_list=fake_task_list)
                future = futures.Future()

                # start a dedicated process to handle the fake activity
                run_fake_task_worker(self.domain.name, fake_task_list, former_event)

        # back to normal execution flow
        if event:
            ttf = self.EVENT_TYPE_TO_FUTURE.get(event['type'])
            if ttf:
                future_and_more = ttf(self, a_task, event)
                if isinstance(future_and_more, tuple):
                    future, a_task = future_and_more
                else:
                    future = future_and_more
            if event['type'] == 'activity':
                if future and future.state in (futures.PENDING, futures.RUNNING):
                    self._open_activity_count += 1

        if not future:
            self.schedule_task(a_task, task_list=self.task_list)
            future = futures.Future()  # return a pending future.

        if self._open_activity_count == constants.MAX_OPEN_ACTIVITY_COUNT:
            logger.warning('limit of {} open activities reached'.format(
                constants.MAX_OPEN_ACTIVITY_COUNT))
            raise exceptions.ExecutionBlocked

        return future

    def _compute_priority(self, priority_set_on_submit, a_task):
        """
        Computes the correct task priority, with the following precedence (first
        is better/preferred):
        - priority set with self.submit(..., __priority=<N>)
        - priority set on the activity task decorator if any
        - priority set on the workflow execution
        - None otherwise

        :param priority_set_on_submit:
        :type  priority_set_on_submit: str|int|PRIORITY_NOT_SET

        :param a_task:
        :type  a_task: ActivityTask|WorkflowTask

        :returns: the priority for this task
        :rtype: str|int|None
        """
        if priority_set_on_submit is not PRIORITY_NOT_SET:
            return priority_set_on_submit
        elif isinstance(a_task, ActivityTask) and a_task.activity.task_priority is not PRIORITY_NOT_SET:
            return a_task.activity.task_priority
        elif self._workflow.task_priority is not PRIORITY_NOT_SET:
            return self._workflow.task_priority
        return None

    def submit(self, func, *args, **kwargs):
        """Register a function and its arguments for asynchronous execution.

        ``*args`` and ``**kwargs`` must be serializable in JSON.
        :type func: simpleflow.base.Submittable | Activity | Workflow

        """
        # NB: we don't set self.current_priority here directly, because we need
        # to extract it from the underlying Activity() if it's not passed to
        # self.submit() ; we DO need to pop the "__priority" kwarg though, so it
        # doesn't pollute the rest of the code.
        priority_set_on_submit = kwargs.pop("__priority", PRIORITY_NOT_SET)

        # casts simpleflow.task.*Task to their equivalent in simpleflow.swf.task
        if not isinstance(func, SwfTask):
            if isinstance(func, base_task.ActivityTask):
                func = ActivityTask.from_generic_task(func)
            elif isinstance(func, base_task.WorkflowTask):
                func = WorkflowTask.from_generic_task(func)
            elif isinstance(func, base_task.SignalTask):
                func = SignalTask.from_generic_task(func, self._workflow_id, self._run_id, None, None)
            elif isinstance(func, base_task.MarkerTask):
                func = MarkerTask.from_generic_task(func)
            elif isinstance(func, base_task.TimerTask):
                func = TimerTask.from_generic_task(func)
            elif isinstance(func, base_task.CancelTimerTask):
                func = CancelTimerTask.from_generic_task(func)

        try:
            # do not use directly "Submittable" here because we want to catch if
            # we don't have an instance from a class known to work under simpleflow.swf
            if isinstance(func, SwfTask):
                # no need to wrap it, already wrapped in the correct format
                a_task = func
            elif isinstance(func, Activity):
                a_task = ActivityTask(func, *args, **kwargs)
            elif issubclass_(func, Workflow):
                a_task = WorkflowTask(self, func, *args, **kwargs)
            elif isinstance(func, WaitForSignal):
                future = self.get_future_from_signal(func.signal_name)
                logger.debug('submitted WaitForSignalTask({}): future={}'.format(func.signal_name, future))
                if not future.done:
                    self._decisions_and_context.append_kv_to_set_context('waiting_signals', func.signal_name)
                return future
            elif isinstance(func, Submittable):
                raise TypeError(
                    'invalid type Submittable {} for {} (you probably wanted a simpleflow.swf.task.*Task)'.format(
                        type(func), func))
            else:
                raise TypeError('invalid type {} for {}'.format(
                    type(func), func))
        except exceptions.ExecutionBlocked:
            return futures.Future()

        # extract priority now that we have a *Task
        self.current_priority = self._compute_priority(priority_set_on_submit, a_task)

        # finally resume task
        return self.resume(a_task, *a_task.args, **a_task.kwargs)

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

    def replay(self, decision_response, decref_workflow=True):
        # type: (swf.responses.Response, bool) -> DecisionsAndContext
        """Replay the workflow from the start until it blocks.
        Called by the DeciderWorker.

        :param decision_response: an object wrapping the PollForDecisionTask response
        :param decref_workflow : Decref workflow once replay is done (to save memory)

        :returns: a list of decision with an optional context
        """
        self.reset()

        # noinspection PyUnresolvedReferences
        history = decision_response.history
        self._history = History(history)
        self._history.parse()
        self.build_run_context(decision_response)
        # noinspection PyUnresolvedReferences
        self._execution = decision_response.execution

        workflow_started_event = history[0]
        input = workflow_started_event.input
        if input is None:
            input = {}
        args = input.get('args', ())
        kwargs = input.get('kwargs', {})

        self.before_replay()

        try:
            if self._history.cancel_requested:
                decisions = self.handle_cancel_requested()
                if decisions is not None:
                    self.after_replay()
                    self.after_closed()
                    if decref_workflow:
                        self.decref_workflow()
                    return DecisionsAndContext(decisions)
            self.propagate_signals()
            result = self.run_workflow(*args, **kwargs)
        except exceptions.ExecutionBlocked:
            logger.info('{} open activities ({} decisions)'.format(
                self._open_activity_count,
                len(self._decisions_and_context.decisions),
            ))
            self.after_replay()
            if decref_workflow:
                self.decref_workflow()
            if self._append_timer:
                self._add_start_timer_decision('_simpleflow_wake_up_timer')

            if not self._decisions_and_context.execution_context:
                self.maybe_clear_execution_context()

            return self._decisions_and_context
        except (exceptions.TaskException, exceptions.WorkflowException) as err:
            def _extract_reason(err):
                if hasattr(err.exception, 'reason'):
                    raw = err.exception.reason
                    # don't parse eventual json object here, since we will cast
                    # the result to a string anyway, better keep a json representation
                    return format.decode(raw, parse_json=False, use_proxy=False)
                return repr(err.exception)

            reason = 'Workflow execution error in {}: "{}"'.format(
                err.payload.name,
                _extract_reason(err))
            logger.exception('%s', reason)  # Don't let logger try to interpolate the message

            details = getattr(err.exception, 'details', None)
            self.on_failure(reason, details)

            decision = swf.models.decision.WorkflowExecutionDecision()
            decision.fail(
                reason=reason,
                details=details,
            )
            self.after_closed()
            if decref_workflow:
                self.decref_workflow()
            return DecisionsAndContext([decision])

        except Exception as err:
            reason = 'Cannot replay the workflow: {}({})'.format(
                err.__class__.__name__,
                err,
            )

            tb = traceback.format_exc()
            details = 'Traceback:\n{}'.format(tb)
            logger.exception('%s', reason + '\n' + details)  # Don't let logger try to interpolate the message

            self.on_failure(reason)

            decision = swf.models.decision.WorkflowExecutionDecision()
            decision.fail(
                reason=reason,
                details=details,
            )
            self.after_closed()
            if decref_workflow:
                self.decref_workflow()
            return DecisionsAndContext([decision])

        self.after_replay()
        decision = swf.models.decision.WorkflowExecutionDecision()
        decision.complete(result=result)
        self.on_completed()
        self.after_closed()
        if decref_workflow:
            self.decref_workflow()
        return DecisionsAndContext([decision])

    def maybe_clear_execution_context(self):
        """
        Replace a null execution_context with an empty string if the preceding one was set.
        This is to clear latestExecutionContext.
        :return:
        """
        events = self._history.events
        last_completed_decision = next(
            # next((generator), default) to prevent StopIteration. Python is fun :-)
            (e for e in reversed(events) if e.type == 'DecisionTask' and e.state == 'completed'),
            None
        )
        last_decision_had_context = (
                last_completed_decision and
                hasattr(last_completed_decision, 'execution_context') and
                last_completed_decision.execution_context)
        if last_decision_had_context:
            self._decisions_and_context.execution_context = ""

    def decref_workflow(self):
        """
        Set the `_workflow` ivar to None in the hope of reducing memory consumption.
        """
        self._workflow = None

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

    def on_canceled(self):
        self._workflow.on_canceled(self._history)

    def fail(self, reason, details=None):
        self.on_failure(reason, details)

        decision = swf.models.decision.WorkflowExecutionDecision()
        decision.fail(
            reason='Workflow execution failed: {}'.format(reason),
            details=details,
        )

        self._decisions_and_context.append_decision(decision)
        raise exceptions.ExecutionBlocked('workflow execution failed')

    def run(self, decision_response):
        return self.replay(decision_response)

    def get_run_context(self):
        return self._run_context

    def build_run_context(self, decision_response):
        """
        Extract data from the execution and history.
        :param decision_response:
        :type  decision_response: swf.responses.Response
        """
        # noinspection PyUnresolvedReferences
        execution = decision_response.execution
        if not execution:
            # For tests that don't provide an execution object.
            return

        # noinspection PyUnresolvedReferences
        history = decision_response.history
        workflow_started_event = history[0]
        self._run_context = {
            'name': execution.workflow_type.name,
            'version': execution.workflow_type.version,
            'domain_name': self.domain.name,
            'workflow_id': execution.workflow_id,
            'run_id': execution.run_id,
            'tag_list': getattr(workflow_started_event, 'tag_list', None) or [],
            'continued_execution_run_id': getattr(workflow_started_event, 'continued_execution_run_id', None),
            'parent_workflow_id': getattr(workflow_started_event, 'parent_workflow_execution', {}).get('workflowId'),
            'parent_run_id': getattr(workflow_started_event, 'parent_workflow_execution', {}).get('runId')
        }

    @property
    def _workflow_id(self):
        return self._run_context.get('workflow_id')

    @property
    def _run_id(self):
        return self._run_context.get('run_id')

    def signal(self, name, *args, **kwargs):
        """
        Send a signal.
        Pop workflow_id, run_id and propagate (default: True) from the kwargs.
        If workflow_id is not set or falsy, use the current workflow_id/run_id.
        :param name:
        :param args:
        :param kwargs:
        :return:
        """
        workflow_id = kwargs.pop('workflow_id', None)
        run_id = kwargs.pop('run_id', None)
        propagate = kwargs.pop('propagate', True)
        logger.debug('signal: name={name}, workflow_id={workflow_id}, run_id={run_id}, propagate={propagate}'.format(
            name=name,
            workflow_id=workflow_id if workflow_id else self._workflow_id,
            run_id=run_id if workflow_id else self._run_id,
            propagate=propagate,
        ))

        extra_input = {'__propagate': propagate if isinstance(propagate, bool) else str(propagate)}
        return SignalTask(
            name,
            workflow_id=workflow_id if workflow_id else self._workflow_id,
            run_id=run_id if workflow_id else self._run_id,
            extra_input=extra_input,
            *args,
            **kwargs
        )

    def wait_signal(self, name):
        logger.debug('{} - wait_signal({})'.format(self._workflow_id, name))
        return WaitForSignal(name)

    def propagate_signals(self):
        """
        Send every signals we got to our parent and children.
        Don't send to workflows present in history.signaled_workflows.
        """
        history = self._history
        if not history.signals:
            return

        known_workflows_ids = []
        if self._run_context.get('parent_workflow_id'):
            known_workflows_ids.append(
                (self._run_context['parent_workflow_id'], self._run_context['parent_run_id'])
            )
        known_workflows_ids.extend(
            (w['workflow_id'], w['run_id']) for w in history.child_workflows.values() if w['state'] == 'started'
        )

        known_workflows_ids = frozenset(known_workflows_ids)

        signals_scheduled = False

        for signal in history.signals.values():
            input = signal['input']
            if not isinstance(input, dict):  # foreign signal: don't try processing it
                continue
            propagate = input.get('__propagate', False)
            if not propagate:
                continue
            name = signal['name']

            args = input.get('args', ())
            kwargs = input.get('kwargs', {})
            sender = (
                signal['external_workflow_id'],
                signal['external_run_id']
            )
            signaled_workflows_ids = set(
                (w['workflow_id'], w['run_id']) for w in history.signaled_workflows[name]
            )
            not_signaled_workflows_ids = list(known_workflows_ids - signaled_workflows_ids - {sender})
            extra_input = {'__propagate': propagate}
            for workflow_id, run_id in not_signaled_workflows_ids:
                self.schedule_task(SignalTask(
                    name,
                    workflow_id,
                    run_id,
                    None,
                    extra_input,
                    *args,
                    **kwargs
                ))
                signals_scheduled = True
        if signals_scheduled:
            raise exceptions.ExecutionBlocked()

    def record_marker(self, name, details=None):
        return MarkerTask(name, details)

    def list_markers(self, all=False):
        if all:
            return [
                Marker(m['name'], format.decode(m['details']))
                for ml in self._history.markers.values() for m in ml
            ]
        rc = []
        for ml in self._history.markers.values():
            m = ml[-1]
            if m['state'] == 'recorded':
                rc.append(Marker(m['name'], format.decode(m['details'])))
        return rc

    def get_event_details(self, event_type, event_name):
        if event_type == 'signal':
            return self._history.signals.get(event_name)
        elif event_type == 'marker':
            marker_list = self._history.markers.get(event_name)
            if not marker_list:
                return None
            marker_list = list(
                filter(
                    lambda m: m['state'] == 'recorded',
                    marker_list
                )
            )
            if not marker_list:
                return None
            # Make pleasing details
            marker = copy.copy(marker_list[-1])
            marker['details'] = format.decode(marker['details'])
            return marker
        elif event_type == 'timer':
            return self._history.timers.get(event_name)
        else:
            raise ValueError('Unimplemented type {!r} for get_event_details'.format(
                event_type
            ))

    def handle_cancel_requested(self):
        decision = swf.models.decision.WorkflowExecutionDecision()
        is_current_decision = self._history.completed_decision_id < self._history.cancel_requested_id
        should_cancel = self._workflow.should_cancel(self._history)
        if not should_cancel:
            return None  # ignore cancel
        if is_current_decision:
            self.on_canceled()
            decision.cancel()
            return [decision]
        if self._history.cancel_failed:
            logger.warning('failed: %s', self._history.cancel_failed)
        if (self._history.cancel_failed and
                self._history.cancel_failed_decision_task_completed_event_id == self._history.completed_decision_id):
            # Per http://docs.aws.amazon.com/amazonswf/latest/apireference/API_Decision.html,
            # we should call RespondDecisionTaskCompleted without any decisions; however this hangs the workflow...

            # <1 WorkflowExecution : started>, <2 DecisionTask : scheduled>, <3 DecisionTask : started>,
            # <4 DecisionTask : completed>, <5 ActivityTask : scheduled>, <6 ActivityTask : started>,
            # <7 WorkflowExecution : cancel_requested>, <8 DecisionTask : scheduled>, <9 DecisionTask : started>,
            # <10 ActivityTask : completed>, <11 DecisionTask : scheduled>, <12 DecisionTask : completed>,
            # <13 WorkflowExecution : cancel_failed>, <14 DecisionTask : started>

            # return []
            pass
        decision.cancel()
        return [decision]
