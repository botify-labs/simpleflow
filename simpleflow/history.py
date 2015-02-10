import collections


class History(object):
    def __init__(self, history):
        self._history = history
        self._activities = collections.OrderedDict()
        self._child_workflows = collections.OrderedDict()
        self._tasks = collections.OrderedDict()

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
            }
            if event.activity_id not in self._activities:
                id_ = event.activity_id
                self._activities[id_] = activity
                self._tasks[id_] = self._activities[id_]
            else:
                # When the executor retries a task, it schedules it again.
                # We have to take care of not overriding some values set by the
                # previous execution of the task such as the number of retries
                # in ``retry``.  As the state of the event mutates, it
                # corresponds to the last execution.
                self._activities[event.activity_id].update(activity)
        elif event.state == 'schedule_failed':
            self._activities[event.activity_id] = {
                'type': 'activity',
                'state': event.state,
                'cause': event.cause,
                'activity_type': event.activity_type.copy(),
            }
        elif event.state == 'started':
            activity = get_activity(event)
            activity['state'] = event.state
            activity['identity'] = event.identity
            activity['started_id'] = event.id
        elif event.state == 'completed':
            activity = get_activity(event)
            activity['state'] = event.state
            activity['result'] = event.result
            activity['completed_id'] = event.id
        elif event.state == 'timed_out':
            activity = get_activity(event)
            activity['state'] = event.state
            activity['timeout_type'] = event.timeout_type
            activity['timeout_value'] = getattr(
                events[activity['scheduled_id'] - 1],
                '{}_timeout'.format(event.timeout_type.lower()))
            activity['timed_out_id'] = event.id
            if 'retry' not in activity:
                activity['retry'] = 0
            else:
                activity['retry'] += 1
        elif event.state == 'failed':
            activity = get_activity(event)
            activity['state'] = event.state
            activity['reason'] = event.reason
            activity['details'] = event.details
            if 'retry' not in activity:
                activity['retry'] = 0
            else:
                activity['retry'] += 1
        elif event.state == 'cancelled':
            activity = get_activity(event)
            activity['state'] = event.state
            if hasattr(event, 'details'):
                activity['details'] = event.details

    def parse_child_workflow_event(self, events, event):
        """Aggregate all the attributes of a workflow in a single entry.

        Missing event state are:

            - start_failed
            - failed
            - timed_out
            - canceled
            - terminated

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
            id_ = event.workflow_id
            self._child_workflows[id_] = workflow
            self._tasks[id_] = self._child_workflows[id_]
        elif event.state == 'started':
            workflow = get_workflow(event)
            workflow['state'] = event.state
            workflow['run_id'] = event.workflow_execution['runId']
            workflow['workflow_id'] = event.workflow_execution['workflowId']
            workflow['started_id'] = event.id
        elif event.state == 'completed':
            workflow = get_workflow(event)
            workflow['state'] = event.state
            workflow['result'] = event.result
            workflow['completed_id'] = event.id

    def parse(self):
        events = self.events
        for event in events:
            if event.type == 'ActivityTask':
                self.parse_activity_event(events, event)
            elif event.type == 'ChildWorkflowExecution':
                self.parse_child_workflow_event(events, event)
            else:
                pass
