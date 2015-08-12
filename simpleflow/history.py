import collections


class History(object):
    def __init__(self, history):
        self._history = history
        self._activities = collections.OrderedDict()
        self._child_workflows = collections.OrderedDict()
        self._tasks = []

    @property
    def events(self):
        return self._history.events

    def parse_activity_event(self, events, event):
        """Aggregate all the attributes of an activity in a single entry.

        """
        def get_activity(event):
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
            activity = get_activity(event)
            activity['state'] = event.state
            activity['identity'] = event.identity
            activity['started_id'] = event.id
            activity['started_timestamp'] = event.timestamp
        elif event.state == 'completed':
            activity = get_activity(event)
            activity['state'] = event.state
            activity['result'] = event.result
            activity['completed_id'] = event.id
            activity['completed_timestamp'] = event.timestamp
        elif event.state == 'timed_out':
            activity = get_activity(event)
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
            activity = get_activity(event)
            activity['state'] = event.state
            activity['reason'] = event.reason
            activity['details'] = getattr(event, 'details', '')
            activity['failed_timestamp'] = event.timestamp
            if 'retry' not in activity:
                activity['retry'] = 0
            else:
                activity['retry'] += 1
        elif event.state == 'cancelled':
            activity = get_activity(event)
            activity['state'] = event.state
            if hasattr(event, 'details'):
                activity['details'] = event.details
            activity['cancelled_timestamp'] = event.timestamp

    def parse_child_workflow_event(self, events, event):
        """Aggregate all the attributes of a workflow in a single entry.

        Missing event state are:

            - start_failed
            - failed
            - timed_out
            - canceled
            - terminated

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

        """
        def get_workflow(event):
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
            }
            workflow['initiated_event_timestamp'] = event.timestamp
            if event.workflow_id not in self._child_workflows:
                self._child_workflows[event.workflow_id] = workflow
                self._tasks.append(workflow)
            else:
                self._child_workflows[event.workflow_id].update(workflow)
        elif event.state == 'start_failed':
            workflow = {
                'type': 'child_workflow',
                'state': event.state,
                'cause': event.cause,
                'workflow_type': event.workflow_type.copy(),
                'start_failed_timestamp': event.timestamp,
            }
            if event.workflow_id not in self._child_workflows:
                self._child_workflows[event.workflow_id] = workflow
                self._tasks.append(workflow)
            else:
                self._child_workflows[event.workflow_id].update(workflow)

        elif event.state == 'started':
            workflow = get_workflow(event)
            workflow['state'] = event.state
            workflow['run_id'] = event.workflow_execution['runId']
            workflow['workflow_id'] = event.workflow_execution['workflowId']
            workflow['started_id'] = event.id
            workflow['started_timestamp'] = event.timestamp
        elif event.state == 'completed':
            workflow = get_workflow(event)
            workflow['state'] = event.state
            workflow['result'] = event.result
            workflow['completed_id'] = event.id
            workflow['completed_timestamp'] = event.timestamp
        elif event.state == 'failed':
            workflow = get_workflow(event)
            workflow['state'] = event.state
            workflow['reason'] = event.reason
            workflow['details'] = event.details
            workflow['failed_id'] = event.id
            workflow['failed_timestamp'] = event.timestamp
        elif event.state == 'timed_out':
            workflow = get_workflow(event)
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
            workflow = get_workflow(event)
            workflow['state'] = event.state
            workflow['details'] = getattr(event, 'details', None)
            workflow['canceled_id'] = event.id
            workflow['canceled_timestamp'] = event.timestamp
        elif event.state == 'terminated':
            workflow = get_workflow(event)
            workflow['state'] = event.state
            workflow['terminated_id'] = event.id
            workflow['terminated_timestamp'] = event.timestamp

    def parse(self):
        events = self.events
        for event in events:
            if event.type == 'ActivityTask':
                self.parse_activity_event(events, event)
            elif event.type == 'ChildWorkflowExecution':
                self.parse_child_workflow_event(events, event)
            else:
                pass
