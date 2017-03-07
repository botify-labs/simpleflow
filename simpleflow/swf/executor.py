from __future__ import absolute_import

import hashlib
import json
import logging
import multiprocessing
import re
import traceback

import simpleflow.task as base_task
import swf.exceptions
import swf.format
import swf.models
import swf.models.decision
from simpleflow import (
    exceptions,
    executor,
    futures,
    task,
)
from simpleflow.activity import Activity, PRIORITY_NOT_SET
from simpleflow.base import Submittable
from simpleflow.history import History
from simpleflow.marker import Marker
from simpleflow.signal import WaitForSignal
from simpleflow.swf import constants
from simpleflow.swf.helpers import swf_identity
from simpleflow.swf.task import ActivityTask, WorkflowTask, SignalTask, MarkerTask, SwfTask
from simpleflow.utils import (
    hex_hash,
    issubclass_,
    json_dumps,
    json_loads_or_raw,
    retry,
)
from simpleflow.workflow import Workflow
from swf.core import ConnectedSWFObject

logger = logging.getLogger(__name__)

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


# TODO: test that correctly! At the time of writing this I don't have any real
# world crawl containing child workflows, so this is not guaranteed to work the
# first time, and it's a bit hard to test end-to-end even with moto.mock_swf
# (child workflows are not really well supported there too).
# ---
# if "poll_for_decision_task" doesn't contain a "taskToken"
# key, then retry ; it happens (not often) that the decider
# doesn't get the scheduled task while it should...
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

    """

    def __init__(self, domain, workflow_class, task_list=None, repair_with=None,
                 force_activities=None):
        super(Executor, self).__init__(workflow_class)
        self._history = None
        self._execution_context = {}
        self.domain = domain
        self.task_list = task_list
        self.repair_with = repair_with
        if force_activities:
            self.force_activities = re.compile(force_activities)
        else:
            self.force_activities = None
        self.reset()

    # noinspection PyAttributeOutsideInit
    def reset(self):
        """
        Clears the state of the execution.

        It is required to ensure the id of the tasks are assigned the same way
        on each replay.

        """
        self._open_activity_count = 0
        self._decisions = []
        self._append_timer = False  # Append an immediate timer decision
        self._tasks = TaskRegistry()
        self._idempotent_tasks_to_submit = set()
        self._execution = None
        self.current_priority = None
        self.create_workflow()

    def _make_task_id(self, a_task, *args, **kwargs):
        """
        Assign a new ID to *a_task*.

        :type a_task: ActivityTask | WorkflowTask
        :returns:
            String with at most 256 characters.
        :rtype: str

        """
        if not a_task.idempotent:
            # If idempotency is False or unknown, let's generate a task id by
            # incrementing an id after the a_task name.
            # (default strategy, backwards compatible with previous versions)
            suffix = self._tasks.add(a_task)
        else:
            # If a_task is idempotent, we can do better and hash arguments.
            # It makes the workflow resistant to retries or variations on the
            # same task name (see #11).
            arguments = json_dumps({"args": args, "kwargs": kwargs}, sort_keys=True)
            suffix = hashlib.md5(arguments.encode('utf-8')).hexdigest()

        if isinstance(a_task, (WorkflowTask,)):
            # Some task types must have globally unique names.
            suffix = '{}--{}--{}'.format(self._workflow_id, hex_hash(self._run_id), suffix)

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
            future.set_running()
        elif state == 'completed':
            result = event['result']
            future.set_finished(json_loads_or_raw(result))
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
            future.set_finished(json_loads_or_raw(event['result']))
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
        Get the event corresponding to a activity task, if any.

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
        Get the event corresponding to a activity task, if any.

        :param a_task:
        :type a_task: MarkerTask
        :param history:
        :type history: simpleflow.history.History
        :return:
        :rtype: Optional[dict[str, Any]]
        """
        json_details = a_task.get_json_details()
        marker_list = history.markers.get(a_task.name)
        if not marker_list:
            return None
        marker_list = filter(
            lambda m: m['state'] == 'recorded' and m['details'] == json_details,
            marker_list
        )
        return marker_list[-1] if marker_list else None

    TASK_TYPE_TO_EVENT_FINDER = {
        ActivityTask: find_activity_event,
        WorkflowTask: find_child_workflow_event,
        SignalTask: find_signal_event,
        MarkerTask: find_marker_event,
    }

    def find_event(self, a_task, history):
        """
        Get the event corresponding to an activity or child workflow, if any
        :param a_task:
        :type a_task: ActivityTask | WorkflowTask | SignalTask
        :param history:
        :type history: simpleflow.history.History
        :return:
        :rtype: Optional[dict]
        """
        finder = self.TASK_TYPE_TO_EVENT_FINDER.get(type(a_task))
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
        :rtype: futures.Future | None
        """
        future = self._get_future_from_activity_event(event)
        if not future:  # schedule failed, maybe OK later.
            return None

        if not future.finished:  # Still pending or running...
            return future

        if future.exception is None:  # Result available!
            return future

        # Compare number of retries in history with configured max retries
        # NB: we used to do a strict comparison (==), but that can lead to
        # infinite retries in case the code is redeployed with a decreased
        # retry limit and a workflow has a already crossed the new limit. So
        # ">=" is better there.
        if event.get('retry', 0) >= a_task.activity.retry:
            if a_task.activity.raises_on_failure:
                raise exceptions.TaskException(a_task, future.exception)
            return future  # with future.exception set.

        # Otherwise retry the task by scheduling it again.
        return None  # means the task is not in SWF.

    def resume_child_workflow(self, a_task, event):
        """
        Resume a child workflow.

        :param a_task:
        :type a_task: WorkflowTask
        :param event:
        :type event: dict
        :return:
        :rtype: Optional[simpleflow.futures.Future]
        """
        future = self._get_future_from_child_workflow_event(event)

        if not future:
            # WORKFLOW_TYPE_DOES_NOT_EXIST, will be created
            return None

        if future.finished and future.exception:
            raise future.exception

        return future

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

        # if isinstance(a_task, SignalTask):
        #     if a_task.workflow_id is None:
        #         a_task.workflow_id = self._execution_context['workflow_id']
        #         if a_task.run_id is None:
        #             a_task.run_id = self._execution_context['run_id']

        # NB: ``decisions`` contains a single decision.
        decisions = a_task.schedule(self.domain, task_list, priority=self.current_priority)

        # Ready to schedule
        if isinstance(a_task, ActivityTask):
            self._open_activity_count += 1
        elif isinstance(a_task, MarkerTask):
            self._append_timer = True  # markers don't generate decisions, so force a wake-up timer

        # Check if we won't violate the 1MB limit on API requests ; if so, do NOT
        # schedule the requested task and block execution instead, with a timer
        # to wake up the workflow immediately after completing these decisions.
        # See: http://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dg-limits.html
        request_size = len(json.dumps(self._decisions + decisions))
        # We keep a 5kB of error margin for headers, json structure, and the
        # timer decision, and 32kB for the context, even if we don't use it now.
        if request_size > constants.MAX_REQUEST_SIZE - 5000 - 32000:
            # TODO: at this point we may check that self._decisions is not empty
            # If it's the case, it means that a single decision was weighting
            # more than 900kB, so we have bigger problems.
            self._append_timer = True
            raise exceptions.ExecutionBlocked()

        self._decisions.extend(decisions)

        # Check if we won't exceed max decisions -1
        # TODO: if we had exactly MAX_DECISIONS - 1 to take, this will wake up
        # the workflow for no reason. Evaluate if we can do better.
        if len(self._decisions) == constants.MAX_DECISIONS - 1:
            # We add a timer to wake up the workflow immediately after
            # completing these decisions.
            self._append_timer = True
            raise exceptions.ExecutionBlocked()

    def _add_start_timer_decision(self, id):
        timer = swf.models.decision.TimerDecision(
            'start',
            id=id,
            start_to_fire_timeout='0')
        self._decisions.append(timer)

    EVENT_TYPE_TO_FUTURE = {
        'activity': resume_activity,
        'child_workflow': resume_child_workflow,
        'signal': get_future_from_signal_event,
        'external_workflow': get_future_from_external_workflow_event,
        'marker': _get_future_from_marker_event,
    }

    def resume(self, a_task, *args, **kwargs):
        """Resume the execution of a task.
        Called by `submit`.

        If the task was scheduled, returns a future that wraps its state,
        otherwise schedules it.
        If in repair mode, we may fake the task to repair from the previous history.

        :param a_task:
        :type a_task: ActivityTask | WorkflowTask | SignalTask
        :param args:
        :param args: list
        :type kwargs:
        :type kwargs: dict
        :rtype: futures.Future
        :raise: exceptions.ExecutionBlocked if open activities limit reached
        """

        if not a_task.id:  # Can be already set (WorkflowTask)
            a_task.id = self._make_task_id(a_task, *args, **kwargs)
        event = self.find_event(a_task, self._history)
        logger.debug('executor: resume {}, event={}'.format(a_task, event))
        future = None

        # in repair mode, check if we absolutely want to re-execute this task
        force_execution = (self.force_activities and
                           self.force_activities.search(a_task.id))

        # try to fill in the blanks with the workflow we're trying to repair if any
        # TODO: maybe only do that for idempotent tasks?? (not enough information to decide?)
        if not event and self.repair_with and not force_execution:
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
                future = ttf(self, a_task, event)
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
        elif (isinstance(a_task, ActivityTask) and
              a_task.activity.task_priority is not PRIORITY_NOT_SET):
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

        try:
            # do not use directly "Submittable" here because we want to catch if
            # we don't have an instance from a class known to work under simpleflow.swf
            if isinstance(func, (ActivityTask, WorkflowTask, SignalTask, MarkerTask)):
                # no need to wrap it, already wrapped in the correct format
                a_task = func
            elif isinstance(func, Activity):
                a_task = ActivityTask(func, *args, **kwargs)
            elif issubclass_(func, Workflow):
                a_task = WorkflowTask(self, func, *args, **kwargs)
            elif isinstance(func, WaitForSignal):
                future = self.get_future_from_signal(func.signal_name)
                logger.debug('submitted WaitForSignalTask({}): future={}'.format(func.signal_name, future))
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
        """Replay the workflow from the start until it blocks.
        Called by the DeciderWorker.

        :param decision_response: an object wrapping the PollForDecisionTask response
        :type  decision_response: swf.responses.Response
        :param decref_workflow : Decref workflow once replay is done (to save memory)
        :type decref_workflow : boolean

        :returns: a list of decision and a context dict (obsolete, empty)
        :rtype: ([swf.models.decision.base.Decision], dict)
        """
        self.reset()

        history = decision_response.history
        self._history = History(history)
        self._history.parse()
        self.build_execution_context(decision_response)
        self._execution = decision_response.execution

        workflow_started_event = history[0]
        input = workflow_started_event.input
        if input is None:
            input = {}
        args = input.get('args', ())
        kwargs = input.get('kwargs', {})

        self.before_replay()
        try:
            self.propagate_signals()
            result = self.run_workflow(*args, **kwargs)
        except exceptions.ExecutionBlocked:
            logger.info('{} open activities ({} decisions)'.format(
                self._open_activity_count,
                len(self._decisions),
            ))
            self.after_replay()
            if decref_workflow:
                self.decref_workflow()
            if self._append_timer:
                self._add_start_timer_decision('_simpleflow_wake_up_timer')
            return self._decisions, {}
        except exceptions.TaskException as err:
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
            if decref_workflow:
                self.decref_workflow()
            return [decision], {}

        except Exception as err:
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
            if decref_workflow:
                self.decref_workflow()
            return [decision], {}

        self.after_replay()
        decision = swf.models.decision.WorkflowExecutionDecision()
        decision.complete(result=swf.format.result(json_dumps(result)))
        self.on_completed()
        self.after_closed()
        if decref_workflow:
            self.decref_workflow()
        return [decision], {}

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

    def get_execution_context(self):
        return self._execution_context

    def build_execution_context(self, decision_response):
        """
        Extract data from the execution and history.
        :param decision_response:
        :type  decision_response: swf.responses.Response
        """
        execution = decision_response.execution
        if not execution:
            # For tests that don't provide an execution object.
            return

        history = decision_response.history
        workflow_started_event = history[0]
        self._execution_context = dict(
            name=execution.workflow_type.name,
            version=execution.workflow_type.version,
            workflow_id=execution.workflow_id,
            run_id=execution.run_id,
            tag_list=getattr(workflow_started_event, 'tag_list', None) or [],  # attribute is absent if no tagList
            continued_execution_run_id=getattr(workflow_started_event, 'continued_execution_run_id', None),
            parent_workflow_id=getattr(workflow_started_event, 'parent_workflow_execution', {}).get('workflowId'),
            parent_run_id=getattr(workflow_started_event, 'parent_workflow_execution', {}).get('runId'),
        )

    @property
    def _workflow_id(self):
        return self._execution_context.get('workflow_id')

    @property
    def _run_id(self):
        return self._execution_context.get('run_id')

    def signal(self, name, workflow_id=None, run_id=None, propagate=True, *args, **kwargs):
        """
        Send a signal.
        :param name:
        :param workflow_id:
        :param run_id:
        :param propagate:
        :param args:
        :param kwargs:
        :return:
        """
        logger.debug('signal: name={name}, workflow_id={workflow_id}, run_id={run_id}, propagate={propagate}'.format(
            name=name,
            workflow_id=workflow_id if workflow_id else self._workflow_id,
            run_id=run_id if workflow_id else self._run_id,
            propagate=propagate,
        ))

        extra_input = {'__propagate': False} if not propagate else None
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
        if self._execution_context['parent_workflow_id']:
            known_workflows_ids.append(
                (self._execution_context['parent_workflow_id'], self._execution_context['parent_run_id'])
            )
        known_workflows_ids.extend(
            (w['workflow_id'], w['run_id']) for w in history.child_workflows.values() if w['state'] == 'started'
        )

        known_workflows_ids = frozenset(known_workflows_ids)

        for signal in history.signals.values():
            input = signal['input']
            propagate = input.get('__propagate', True)
            if not propagate:
                continue
            name = signal['name']
            orig_workflow_id = input.get('__workflow_id')
            orig_run_id = input.get('__run_id')

            input = {
                'args': input.get('args'),
                'kwargs': input.get('kwargs'),
                '__workflow_id': self._workflow_id,
                '__run_id': self._run_id,
            }
            sender = (
                signal['external_workflow_id'] or orig_workflow_id,
                signal['external_run_id'] or orig_run_id
            )
            signaled_workflows_ids = set(
                (w['workflow_id'], w['run_id']) for w in history.signaled_workflows[name]
            )
            signaled_workflows_ids.add((orig_workflow_id, orig_run_id))
            not_signaled_workflows_ids = list(known_workflows_ids - signaled_workflows_ids - {sender})
            for workflow_id, run_id in not_signaled_workflows_ids:
                try:
                    self._execution.signal(
                        signal_name=name,
                        input=input,
                        workflow_id=workflow_id,
                        run_id=run_id,
                    )
                except swf.models.workflow.WorkflowExecutionDoesNotExist:
                    logger.info('Workflow {} {} disappeared'.format(workflow_id, run_id))

    def record_marker(self, name, details=None):
        return MarkerTask(name, details)

    def list_markers(self, all=False):
        if all:
            return [
                Marker(m['name'], json_loads_or_raw(m['details']))
                for ml in self._history.markers.values() for m in ml
            ]
        rc = []
        for ml in self._history.markers.values():
            m = ml[-1]
            if m['state'] == 'recorded':
                rc.append(Marker(m['name'], json_loads_or_raw(m['details'])))
        return rc
