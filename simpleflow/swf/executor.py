from __future__ import absolute_import

import hashlib
import json
import logging
import multiprocessing
import re
import traceback

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
from simpleflow.activity import Activity
from simpleflow.base import Submittable
from simpleflow.history import History
from simpleflow.swf import constants
from simpleflow.swf.helpers import swf_identity
from simpleflow.swf.task import ActivityTask, WorkflowTask
from simpleflow.task import (
    ActivityTask as BaseActivityTask,
    WorkflowTask as BaseWorkflowTask,
)
from simpleflow.utils import issubclass_, json_dumps, hex_hash
from simpleflow.utils import retry
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
    :ivar workflow: workflow
    :ivar task_list: task list
    :type task_list: Optional[str]
    :ivar repair_with: previous history to use for repairing
    :type repair_with: Optional[simpleflow.history.History]
    :ivar force_activities: regex with activities to force
    :type _history: History

    """

    def __init__(self, domain, workflow, task_list=None, repair_with=None,
                 force_activities=None):
        super(Executor, self).__init__(workflow)
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

    def reset(self):
        """
        Clears the state of the execution.

        It is required to ensure the id of the tasks are assigned the same way
        on each replay.

        """
        self._open_activity_count = 0
        self._decisions = []
        self._tasks = TaskRegistry()
        self._idempotent_tasks_to_submit = set()

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
            future.set_finished(json.loads(result) if result else None)
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
            future.set_finished(json.loads(event['result']))
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

    @staticmethod
    def find_activity_event(a_task, history):
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

    @staticmethod
    def find_child_workflow_event(a_task, history):
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

    def find_event(self, a_task, history):
        """
        Get the event corresponding to an activity or child workflow, if any
        :param a_task:
        :type a_task: ActivityTask | WorkflowTask
        :param history:
        :type history: simpleflow.history.History
        :return:
        :rtype: Optional[dict]
        """
        # FIXME move this
        event_type_to_finder = {
            ActivityTask: self.find_activity_event,
            WorkflowTask: self.find_child_workflow_event,
        }
        finder = event_type_to_finder.get(type(a_task))
        if finder:
            return finder(a_task, history)
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
        :type a_task: ActivityTask | WorkflowTask
        :param task_list:
        :type task_list: Optional[str]
        :return:
        :rtype:
        :raise: exceptions.ExecutionBlocked if too many decisions waiting
        """
        # Don't re-schedule idempotent tasks
        if a_task.idempotent:
            task_identifier = (type(a_task), self.domain, a_task.id)
            if task_identifier in self._idempotent_tasks_to_submit:
                logger.debug('Not resubmitting task {}'.format(a_task.name))
                return
            self._idempotent_tasks_to_submit.add(task_identifier)

        # NB: ``decisions`` contains a single decision.
        decisions = a_task.schedule(self.domain, task_list)

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
            self._add_start_timer_decision('resume-after-{}'.format(a_task.id))
            raise exceptions.ExecutionBlocked()

        # Ready to schedule
        logger.debug('executor is scheduling task {} on task_list {}'.format(
            a_task.name,
            task_list,
        ))
        self._decisions.extend(decisions)
        self._open_activity_count += 1

        # Check if we won't exceed max decisions -1
        # TODO: if we had exactly MAX_DECISIONS - 1 to take, this will wake up
        # the workflow for no reason. Evaluate if we can do better.
        if len(self._decisions) == constants.MAX_DECISIONS - 1:
            # We add a timer to wake up the workflow immediately after
            # completing these decisions.
            self._add_start_timer_decision('resume-after-{}'.format(a_task.id))
            raise exceptions.ExecutionBlocked()

    def _add_start_timer_decision(self, id):
        timer = swf.models.decision.TimerDecision(
            'start',
            id=id,
            start_to_fire_timeout='0')
        self._decisions.append(timer)

    def resume(self, a_task, *args, **kwargs):
        """Resume the execution of a task.
        Called by `submit`.

        If the task was scheduled, returns a future that wraps its state,
        otherwise schedules it.
        If in repair mode, we may fake the task to repair from the previous history.

        :param a_task:
        :type a_task: ActivityTask | WorkflowTask
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
            event_type_to_future = {  # TODO move elsewhere
                'activity': self.resume_activity,
                'child_workflow': self.resume_child_workflow,
            }
            ttf = event_type_to_future.get(event['type'])
            if ttf:
                future = ttf(a_task, event)
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

    def submit(self, func, *args, **kwargs):
        """Register a function and its arguments for asynchronous execution.

        ``*args`` and ``**kwargs`` must be serializable in JSON.
        :type func: simpleflow.base.Submittable | Activity | Workflow

        """
        # casts simpleflow.task.*Task to their equivalent in simpleflow.swf.task
        if isinstance(func, BaseActivityTask):
            func = ActivityTask.from_generic_task(func)
        elif isinstance(func, BaseWorkflowTask):
            func = WorkflowTask.from_generic_task(func)

        try:
            # do not use directly "Submittable" here because we want to catch if
            # we don't have an instance from a class known to work under simpleflow.swf
            if isinstance(func, (ActivityTask, WorkflowTask)):
                # no need to wrap it, already wrapped in the correct format
                a_task = func
            elif isinstance(func, Submittable):
                raise TypeError(
                    'invalid type Submittable {} for {} (you probably wanted a simpleflow.swf.task.*Task)'.format(
                        type(func), func))

            elif isinstance(func, Activity):
                a_task = ActivityTask(func, *args, **kwargs)
            elif issubclass_(func, Workflow):
                a_task = WorkflowTask(self, func, *args, **kwargs)
            else:
                raise TypeError('invalid type {} for {}'.format(
                    type(func), func))
        except exceptions.ExecutionBlocked:
            return futures.Future()

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

    def replay(self, decision_response):
        """Replay the workflow from the start until it blocks.
        Called by the DeciderWorker.

        :param decision_response: an object wrapping the PollForDecisionTask response
        :type  decision_response: swf.responses.Response

        :returns: a list of decision and a context dict (obsolete, empty)
        :rtype: ([swf.models.decision.base.Decision], dict)
        """
        self.reset()

        history = decision_response.history
        self._history = History(history)
        self._history.parse()
        self.build_execution_context(decision_response)

        workflow_started_event = history[0]
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
            return [decision], {}

        self.after_replay()
        decision = swf.models.decision.WorkflowExecutionDecision()
        decision.complete(result=swf.format.result(json_dumps(result)))
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
