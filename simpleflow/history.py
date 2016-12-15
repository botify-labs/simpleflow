import collections
import logging

logger = logging.getLogger(__name__)


# noinspection PyUnresolvedReferences
class History(object):
    """
    History data.

    :ivar _history: raw(ish) history events
    :type _history: swf.models.history.History
    :ivar _activities: activity events
    :type _activities: collections.OrderedDict[str, dict[str, Any]]
    :ivar _child_workflows: child workflow events
    :type _child_workflows: collections.OrderedDict[str, dict[str, Any]]
    :ivar _tasks: ordered list of tasks/etc
    :type _tasks: list[dict[str, Any]]
    """

    def __init__(self, history):
        self._history = history
        self._activities = collections.OrderedDict()
        self._child_workflows = collections.OrderedDict()
        self._tasks = []

    @property
    def activities(self):
        """
        :return: activities
        :rtype: collections.OrderedDict[str, dict[str, Any]]
        """
        return self._activities

    @property
    def child_workflows(self):
        """
        :return: activities
        :rtype: collections.OrderedDict[str, dict[str, Any]]
        """
        return self._child_workflows

    @property
    def tasks(self):
        return self._tasks

    @property
    def events(self):
        """

        :return:
        :rtype: list[swf.models.event.Event]
        """
        return self._history.events

    def parse_activity_event(self, events, event):
        """
        Aggregate all the attributes of an activity in a single entry.

        :param events:
        :type events: list[swf.models.event.Event]
        :param event:
        :type event: swf.models.event.Event
        """

        def get_activity():
            """
            Return a reference to the corresponding activity.
            :return: mutable activity
            :rtype: dict[str, Any]
            """
            scheduled_event = events[event.scheduled_event_id - 1]
            return self._activities[scheduled_event.activity_id]

        if event.state == 'scheduled':
            activity = {
                'type': 'activity',
                'id': event.activity_id,
                'name': event.activity_type['name'],
                'version': event.activity_type['version'],
                'state': event.state,
                'scheduled_id': event.id,
                'scheduled_timestamp': event.timestamp,
                'input': event.input,
                'task_list': event.task_list['name'],
            }
            if event.activity_id not in self._activities:
                self._activities[event.activity_id] = activity
                self._tasks.append(activity)
            else:
                # When the executor retries a task, it schedules it again.
                # We have to take care of not overriding some values set by the
                # previous execution of the task such as the number of retries
                # in ``retry``.  As the state of the event mutates, it
                # corresponds to the last execution.
                self._activities[event.activity_id].update(activity)
        elif event.state == 'schedule_failed':
            activity = {
                'type': 'activity',
                'state': event.state,
                'cause': event.cause,
                'activity_type': event.activity_type.copy(),
                'schedule_failed_timestamp': event.timestamp,
            }

            if event.activity_id not in self._activities:
                self._activities[event.activity_id] = activity
                self._tasks.append(activity)
            else:
                # When the executor retries a task, it schedules it again.
                # We have to take care of not overriding some values set by the
                # previous execution of the task such as the number of retries
                # in ``retry``.  As the state of the event mutates, it
                # corresponds to the last execution.
                self._activities[event.activity_id].update(activity)

        elif event.state == 'started':
            activity = get_activity()
            activity['state'] = event.state
            activity['identity'] = event.identity
            activity['started_id'] = event.id
            activity['started_timestamp'] = event.timestamp
        elif event.state == 'completed':
            activity = get_activity()
            activity['state'] = event.state
            activity['result'] = event.result
            activity['completed_id'] = event.id
            activity['completed_timestamp'] = event.timestamp
        elif event.state == 'timed_out':
            activity = get_activity()
            activity['state'] = event.state
            activity['timeout_type'] = event.timeout_type
            activity['timeout_value'] = getattr(
                events[activity['scheduled_id'] - 1],
                '{}_timeout'.format(event.timeout_type.lower()))
            activity['timed_out_id'] = event.id
            activity['timed_out_timestamp'] = event.timestamp
            if 'retry' not in activity:
                activity['retry'] = 0
            else:
                activity['retry'] += 1
        elif event.state == 'failed':
            activity = get_activity()
            activity['state'] = event.state
            activity['reason'] = event.reason
            activity['details'] = getattr(event, 'details', '')
            activity['failed_timestamp'] = event.timestamp
            if 'retry' not in activity:
                activity['retry'] = 0
            else:
                activity['retry'] += 1
        elif event.state == 'cancelled':
            activity = get_activity()
            activity['state'] = event.state
            if hasattr(event, 'details'):
                activity['details'] = event.details
            activity['cancelled_timestamp'] = event.timestamp

    def parse_child_workflow_event(self, events, event):
        """Aggregate all the attributes of a workflow in a single entry.

        See http://docs.aws.amazon.com/amazonswf/latest/apireference/API_HistoryEvent.html

        - StartChildWorkflowExecutionInitiated: A request was made to start a
          child workflow execution.
        - StartChildWorkflowExecutionFailed: Failed to process
          StartChildWorkflowExecution decision. This happens when the decision
          is not configured properly, for example the workflow type specified
          is not registered.
        - ChildWorkflowExecutionStarted: A child workflow execution was
          successfully started.
        - ChildWorkflowExecutionCompleted: A child workflow execution, started
          by this workflow execution, completed successfully and was closed.
        - ChildWorkflowExecutionFailed: A child workflow execution, started by
          this workflow execution, failed to complete successfully and was
          closed.
        - ChildWorkflowExecutionTimedOut: A child workflow execution, started
          by this workflow execution, timed out and was closed.
        - ChildWorkflowExecutionCanceled: A child workflow execution, started
          by this workflow execution, was canceled and closed.
        - ChildWorkflowExecutionTerminated: A child workflow execution, started
          by this workflow execution, was terminated.

        :param events:
        :type events: list[swf.models.event.Event]
        :param event:
        :type event: swf.models.event.Event
        """

        def get_workflow():
            initiated_event = events[event.initiated_event_id - 1]
            return self._child_workflows[initiated_event.workflow_id]

        if event.state == 'start_initiated':
            workflow = {
                'type': 'child_workflow',
                'id': event.workflow_id,
                'name': event.workflow_type['name'],
                'version': event.workflow_type['version'],
                'state': event.state,
                'initiated_event_id': event.id,
                'raw_input': event.raw.get('input'),
                'child_policy': event.child_policy,
                'control': getattr(event, 'control', None),
                'tag_list': getattr(event, 'tag_list', None),
                'task_list': event.task_list['name'],
                'initiated_event_timestamp': event.timestamp,
            }
            if event.workflow_id not in self._child_workflows:
                self._child_workflows[event.workflow_id] = workflow
                self._tasks.append(workflow)
            else:
                # May have gotten a start_failed before (or retrying?)
                if self._child_workflows[event.workflow_id]['state'] == 'start_initiated':
                    # Should not happen anymore
                    logger.warning("start_initiated again for workflow {} (initiated @{}, we're @{})".format(
                        event.workflow_id,
                        self._child_workflows[event.workflow_id]['initiated_event_id'],
                        event.id
                    ))
                self._child_workflows[event.workflow_id].update(workflow)
        elif event.state == 'start_failed':
            workflow = {
                'type': 'child_workflow',
                'id': event.workflow_id,
                'state': event.state,
                'cause': event.cause,
                'name': event.workflow_type['name'],
                'version': event.workflow_type['version'],
                'control': getattr(event, 'control', None),
                'start_failed_id': event.id,
                'start_failed_timestamp': event.timestamp,
            }
            if event.workflow_id not in self._child_workflows:
                self._child_workflows[event.workflow_id] = workflow
                self._tasks.append(workflow)
            else:
                self._child_workflows[event.workflow_id].update(workflow)
        elif event.state == 'started':
            workflow = get_workflow()
            workflow['state'] = event.state
            workflow['run_id'] = event.workflow_execution['runId']
            workflow['workflow_id'] = event.workflow_execution['workflowId']
            workflow['started_id'] = event.id
            workflow['started_timestamp'] = event.timestamp
        elif event.state == 'completed':
            workflow = get_workflow()
            workflow['state'] = event.state
            workflow['result'] = event.result
            workflow['completed_id'] = event.id
            workflow['completed_timestamp'] = event.timestamp
        elif event.state == 'failed':
            workflow = get_workflow()
            workflow['state'] = event.state
            workflow['reason'] = getattr(event, 'reason', None)
            workflow['details'] = getattr(event, 'details', None)
            workflow['failed_id'] = event.id
            workflow['failed_timestamp'] = event.timestamp
            # FIXME add retry here too?
        elif event.state == 'timed_out':
            workflow = get_workflow()
            workflow['state'] = event.state
            workflow['timeout_type'] = event.timeout_type
            workflow['timeout_value'] = getattr(
                events[workflow['initiated_event_id'] - 1],
                '{}_timeout'.format(event.timeout_type.lower()),
                None,
            )
            workflow['timed_out_id'] = event.id
            workflow['timed_out_timestamp'] = event.timestamp
            if 'retry' not in workflow:
                workflow['retry'] = 0
            else:
                workflow['retry'] += 1
        elif event.state == 'canceled':
            workflow = get_workflow()
            workflow['state'] = event.state
            workflow['details'] = getattr(event, 'details', None)
            workflow['canceled_id'] = event.id
            workflow['canceled_timestamp'] = event.timestamp
        elif event.state == 'terminated':
            workflow = get_workflow()
            workflow['state'] = event.state
            workflow['terminated_id'] = event.id
            workflow['terminated_timestamp'] = event.timestamp

    TYPE_TO_PARSER = {
        'ActivityTask': parse_activity_event,
        'ChildWorkflowExecution': parse_child_workflow_event,
    }

    def parse(self):
        """
        Parse the events.
        Update the corresponding statuses.
        """

        events = self.events
        for event in events:
            parser = self.TYPE_TO_PARSER.get(event.type)
            if parser:
                parser(self, events, event)
