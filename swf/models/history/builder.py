import json

import swf.models
import swf.models.event.workflow
from swf.models.event.factory import EventFactory

DEFAULT_DECIDER_IDENTITY = 'test_decider'
DEFAULT_WORKER_IDENTITY = 'test_worker'
DEFAULT_REASON = 'REASON'
DEFAULT_DETAILS = 'DETAILS'


__all__ = ['History']


FIRST_TIMESTAMP = None
LATEST_TIMESTAMP = None


def new_timestamp_string():
    from time import time
    import random

    global FIRST_TIMESTAMP
    global LATEST_TIMESTAMP

    if FIRST_TIMESTAMP is None:
        FIRST_TIMESTAMP = time()
        timestamp = FIRST_TIMESTAMP
        LATEST_TIMESTAMP = timestamp
    else:
        LATEST_TIMESTAMP += random.random() * 100
        timestamp = LATEST_TIMESTAMP

    return timestamp


CHILD_WORKFLOW_STATES = set(
    swf.models.event.workflow.CompiledChildWorkflowExecutionEvent.states
)


class History(swf.models.History):
    """
    Help to build a history to simulate the execution of a workflow.

    """
    def __init__(self, workflow, input=None, tag_list=None):
        """
        Bootstrap a history with the first events added by SWF.

        :param workflow: workflow to simulate
        :type  workflow: declarative.Workflow
        :param input: JSON serializable dict
        :type  input: dict
        :param tag_list: string of tags (beware not a list)
        :type  tag_list: str

        """
        self._workflow = workflow
        self.events = [
            EventFactory({
                "eventId": 1,
                "eventType": "WorkflowExecutionStarted",
                "eventTimestamp": new_timestamp_string(),
                "workflowExecutionStartedEventAttributes": {
                    "taskList": {
                        "name": workflow.task_list,
                    },
                    "parentInitiatedEventId": 0,
                    "taskStartToCloseTimeout":
                        workflow.decision_tasks_timeout,
                    "childPolicy": "TERMINATE",
                    "executionStartToCloseTimeout":
                        workflow.execution_timeout,
                    "input": json.dumps(input if input else {}),
                    "workflowType": {
                        "name": workflow.name,
                        "version": workflow.version
                    },
                    "tagList": tag_list or getattr(workflow, 'tag_list', None)
                }
            })
        ]
        self.add_decision_task_scheduled()
        self.add_decision_task_started(len(self.events))

    @property
    def last_id(self):
        return len(self.events)

    @property
    def next_id(self):
        return self.last_id + 1

    def add_decision_task(self, execution_context=None):
        if execution_context is None:
            execution_context = {}

        self.add_decision_task_scheduled()
        self.add_decision_task_started(scheduled=self.last_id)
        self.add_decision_task_completed(
            scheduled=self.last_id - 1,
            started=self.last_id,
            execution_context=execution_context)

        return self

    def add_decision_task_scheduled(self):
        self.events.append(EventFactory({
            "eventId": len(self.events) + 1,
            "eventType": "DecisionTaskScheduled",
            "eventTimestamp": new_timestamp_string(),
            "decisionTaskScheduledEventAttributes": {
                "startToCloseTimeout":
                self._workflow.decision_tasks_timeout,
                "taskList": {
                    "name": self._workflow.task_list,
                }
            }
        }))

        return self

    def add_decision_task_started(self, scheduled=None):
        if scheduled is None:
            scheduled = self.last_id

        self.events.append(EventFactory({
            "eventId": len(self.events) + 1,
            "eventType": "DecisionTaskStarted",
            "eventTimestamp": new_timestamp_string(),
            "decisionTaskStartedEventAttributes": {
                "scheduledEventId": scheduled,
                "identity": DEFAULT_DECIDER_IDENTITY,
            }
        }))

        return self

    def add_decision_task_completed(self, scheduled=None, started=None,
                                    execution_context=None):
        if scheduled is None:
            scheduled = self.last_id - 1

        if started is None:
            started = self.last_id

        self.events.append(EventFactory({
            "eventId": len(self.events) + 1,
            "eventType": "DecisionTaskCompleted",
            "eventTimestamp": new_timestamp_string(),
            "decisionTaskCompletedEventAttributes": {
                "startedEventId": started,
                "scheduledEventId": scheduled,
                "executionContext": (json.dumps(execution_context) if
                                     execution_context is not None else None),
            }
        }))

        return self

    def add_decision_task_timed_out(self,
                                    scheduled=None,
                                    started=None,
                                    timeout_type='START_TO_CLOSE'):
        if scheduled is None:
            scheduled = self.last_id - 1

        if started is None:
            started = self.last_id

        self.events.append(EventFactory({
            "eventId": len(self.events) + 1,
            "eventType": "DecisionTaskTimedOut",
            "eventTimestamp": new_timestamp_string(),
            "decisionTaskTimedOutEventAttributes": {
                "startedEventId": started,
                "scheduledEventId": scheduled,
                "timeoutType": timeout_type,
            }
        }))

        return self

    def add_activity_task_schedule_failed(self,
                                          activity_id,
                                          decision_id,
                                          activity_type,
                                          cause):
        self.events.append(EventFactory({
            u'eventId': self.next_id,
            u'eventTimestamp': 1386947268.527,
            u'eventType': u'ScheduleActivityTaskFailed',
            u'scheduleActivityTaskFailedEventAttributes': {
                u'activityId': activity_id,
                u'activityType': activity_type.copy(),
                u'cause': cause,
                u'decisionTaskCompletedEventId': decision_id,
            }
        }))

        return self

    def add_activity_task_scheduled(self, activity, decision_id,
                                    activity_id=None,
                                    input=None,
                                    control=None):
        if control is None:
            control = {}

        self.events.append(EventFactory({
            "eventId": len(self.events) + 1,
            "eventType": "ActivityTaskScheduled",
            "eventTimestamp": new_timestamp_string(),
            "activityTaskScheduledEventAttributes": {
                'control': (json.dumps(control) if
                            control is not None else None),
                "taskList": {
                    "name": activity.task_list,
                },
                "scheduleToCloseTimeout": activity.task_schedule_to_close_timeout,
                "activityType": {
                    "name": activity.name,
                    "version": activity.version,
                },
                "heartbeatTimeout": activity.task_heartbeat_timeout,
                "activityId": (activity_id if activity_id is not None else
                               '{}-{}'.format(
                                   activity.name, hash(activity.name))),
                "scheduleToStartTimeout": activity.task_schedule_to_start_timeout,
                "decisionTaskCompletedEventId": decision_id,
                "input": json.dumps(input if input is not None else {}),
                "startToCloseTimeout": activity.task_start_to_close_timeout,
            }
        }))

        return self

    def add_activity_task_started(self, scheduled):
        self.events.append(EventFactory({
            "eventId": len(self.events) + 1,
            "eventType": "ActivityTaskStarted",
            "eventTimestamp": new_timestamp_string(),
            "activityTaskStartedEventAttributes": {
                "scheduledEventId": scheduled,
                "identity": DEFAULT_WORKER_IDENTITY,
            }
        }))

        return self

    def add_activity_task_completed(self, scheduled, started,
                                    result=None):
        self.events.append(EventFactory({
            "eventId": len(self.events) + 1,
            "eventType": "ActivityTaskCompleted",
            "eventTimestamp": new_timestamp_string(),
            "activityTaskCompletedEventAttributes": {
                "startedEventId": started,
                "scheduledEventId": scheduled,
                "result": json.dumps(result) if result is not None else None,
            }
        }))

        return self

    def add_activity_task_failed(self,
                                 scheduled=None,
                                 started=None,
                                 reason=DEFAULT_REASON,
                                 details=DEFAULT_DETAILS):
        self.events.append(EventFactory({
            'eventId': self.next_id,
            'eventType': 'ActivityTaskFailed',
            'eventTimestamp': new_timestamp_string(),
            'activityTaskFailedEventAttributes': {
                'reason': reason,
                'details': details,
                'scheduledEventId': (scheduled if scheduled is not None else
                                     self.last_id - 1),
                'startedEventId': (started if started is not None else
                                   self.last_id)
            }
        }))

        return self

    def add_activity_task_timed_out(self,
                                    timeout_type,
                                    scheduled=None,
                                    started=None):
        if scheduled is None:
            scheduled = self.last_id - 1

        if started is None:
            started = self.last_id

        self.events.append(EventFactory({
            'eventId': self.next_id,
            'eventTimestamp': new_timestamp_string(),
            'eventType': 'ActivityTaskTimedOut',
            'activityTaskTimedOutEventAttributes': {
                'scheduledEventId': scheduled,
                'startedEventId': started,
                'timeoutType': timeout_type,
            }
        }))

    def add_activity_task(self,
                          activity,
                          decision_id,
                          last_state='completed',
                          activity_id=None,
                          input=None,
                          control=None,
                          result=None,
                          reason=DEFAULT_REASON,
                          details=DEFAULT_DETAILS,
                          activity_type=None,
                          cause=None,
                          timeout_type='START_TO_CLOSE'):
        self.add_activity_task_scheduled(
            activity,
            decision_id,
            activity_id,
            input,
            control)
        if last_state == 'scheduled':
            return self

        if last_state == 'schedule_failed':
            self.add_activity_task_schedule_failed(
                activity_id,
                decision_id,
                activity_type,
                cause)
            return self

        scheduled_id = self.last_id
        self.add_activity_task_started(scheduled=scheduled_id)
        if last_state == 'started':
            return self

        started_id = self.last_id
        if last_state == 'completed':
            self.add_activity_task_completed(
                scheduled=scheduled_id,
                started=started_id,
                result=result)
        elif last_state == 'failed':
            self.add_activity_task_failed(
                scheduled=scheduled_id,
                started=started_id,
                reason=reason,
                details=details)
        elif last_state == 'timed_out':
            self.add_activity_task_timed_out(
                scheduled=scheduled_id,
                started=started_id,
                timeout_type=timeout_type)
        else:
            raise ValueError('last state {} is not supported'.format(
                             last_state))

        return self

    def add_child_workflow_start_initiated(self,
                                           workflow,
                                           workflow_id=None,
                                           task_list=None,
                                           input=None,
                                           control=None,
                                           tag_list=None,
                                           task_start_to_close_timeout=0):
        if control is None:
            control = {}

        self.events.append(EventFactory({
            'eventId': self.next_id,
            'eventType': 'StartChildWorkflowExecutionInitiated',
            'eventTimestamp': new_timestamp_string(),
            'startChildWorkflowExecutionInitiatedEventAttributes': {
                'control': (json.dumps(control) if
                            control is not None else None),
                'childPolicy': 'TERMINATE',
                'decisionTaskCompletedEventId': 76,
                'executionStartToCloseTimeout': '432000',
                'input': (json.dumps(input) if
                          input is not None else '{}'),
                'tagList': tag_list,
                'taskList': task_list,
                'taskStartToCloseTimeout': task_start_to_close_timeout,
                'workflowId': workflow_id,
                'workflowType': {
                    'name': workflow.name,
                    'version': workflow.version
                }
            }
        }))

        return self

    def add_child_workflow_started(self,
                                   initiated_id,
                                   name=None,
                                   version=None):
        initiated_event = self.events[initiated_id - 1]
        workflow_id = initiated_event.workflow_id
        workflow_type = initiated_event.workflow_type

        timestamp = new_timestamp_string()
        self.events.append(EventFactory({
            'eventId': self.next_id,
            'eventTimestamp': timestamp,
            'eventType': 'ChildWorkflowExecutionStarted',
            'childWorkflowExecutionStartedEventAttributes': {
                'initiatedEventId': initiated_id,
                'workflowExecution': {
                    'runId': timestamp,
                    'workflowId': workflow_id,
                },
                'workflowType': {
                    'name': workflow_type['name'],
                    'version': workflow_type['version'],
                }
            }
        }))

        return self

    def add_child_workflow_completed(self,
                                     initiated_id,
                                     started_id,
                                     result=None):
        initiated_event = self.events[initiated_id - 1]
        workflow_id = initiated_event.workflow_id
        workflow_type = initiated_event.workflow_type

        started_event = self.events[started_id - 1]
        workflow_execution = started_event.workflow_execution

        self.events.append(EventFactory({
            'eventId': self.next_id,
            'eventTimestamp': new_timestamp_string(),
            'eventType': 'ChildWorkflowExecutionCompleted',
            'childWorkflowExecutionCompletedEventAttributes': {
                'initiatedEventId': initiated_id,
                'result': result,
                'startedEventId': started_id,
                'workflowExecution': {
                    'runId': workflow_execution['runId'],
                    'workflowId': workflow_id
                },
                'workflowType': {
                    'name': workflow_type['name'],
                    'version': workflow_type['version']
                }
            },
        }))

        return self

    def add_child_workflow_failed(self,
                                  initiated_id,
                                  started_id,
                                  reason=None,
                                  details=None):
        initiated_event = self.events[initiated_id - 1]
        workflow_id = initiated_event.workflow_id
        workflow_type = initiated_event.workflow_type

        started_event = self.events[started_id - 1]
        workflow_execution = started_event.workflow_execution

        self.events.append(EventFactory({
            'eventId': self.next_id,
            'eventTimestamp': new_timestamp_string(),
            'eventType': 'ChildWorkflowExecutionFailed',
            'childWorkflowExecutionFailedEventAttributes': {
                'initiatedEventId': initiated_id,
                'startedEventId': started_id,
                'reason': reason,
                'details': details,
                'workflowExecution': {
                    'runId': workflow_execution['runId'],
                    'workflowId': workflow_id,
                },
                'workflowType': {
                    'name': workflow_type['name'],
                    'version': workflow_type['version']
                }
            }
        }))

        return self

    def add_child_workflow_timed_out(self,
                                     initiated_id,
                                     started_id,
                                     timeout_type):
        initiated_event = self.events[initiated_id - 1]
        workflow_id = initiated_event.workflow_id
        workflow_type = initiated_event.workflow_type

        started_event = self.events[started_id - 1]
        workflow_execution = started_event.workflow_execution

        self.events.append(EventFactory({
            'eventId': self.next_id,
            'eventTimestamp': new_timestamp_string(),
            'eventType': 'ChildWorkflowExecutionTimedOut',
            'childWorkflowExecutionTimedOutEventAttributes': {
                'initiatedEventId': initiated_id,
                'startedEventId': started_id,
                'timeoutType': timeout_type,
                'workflowExecution': {
                    'runId': workflow_execution['runId'],
                    'workflowId': workflow_id,
                },
                'workflowType': {
                    'name': workflow_type['name'],
                    'version': workflow_type['version']
                }
            }
        }))

        return self

    def add_child_workflow_canceled(self,
                                    initiated_id,
                                    started_id):
        initiated_event = self.events[initiated_id - 1]
        workflow_id = initiated_event.workflow_id
        workflow_type = initiated_event.workflow_type

        started_event = self.events[started_id - 1]
        workflow_execution = started_event.workflow_execution

        self.events.append(EventFactory({
            'eventId': self.next_id,
            'eventTimestamp': new_timestamp_string(),
            'eventType': 'ChildWorkflowExecutionCanceled',
            'childWorkflowExecutionCanceledEventAttributes': {
                'initiatedEventId': initiated_id,
                'startedEventId': started_id,
                'workflowExecution': {
                    'runId': workflow_execution['runId'],
                    'workflowId': workflow_id,
                },
                'workflowType': {
                    'name': workflow_type['name'],
                    'version': workflow_type['version']
                }
            }
        }))

        return self

    def add_child_workflow_terminated(self,
                                      initiated_id,
                                      started_id):
        initiated_event = self.events[initiated_id - 1]
        workflow_id = initiated_event.workflow_id
        workflow_type = initiated_event.workflow_type

        started_event = self.events[started_id - 1]
        workflow_execution = started_event.workflow_execution

        self.events.append(EventFactory({
            'eventId': self.next_id,
            'eventTimestamp': new_timestamp_string(),
            'eventType': 'ChildWorkflowExecutionTerminated',
            'childWorkflowExecutionTerminatedEventAttributes': {
                'initiatedEventId': initiated_id,
                'startedEventId': started_id,
                'workflowExecution': {
                    'runId': workflow_execution['runId'],
                    'workflowId': workflow_id,
                },
                'workflowType': {
                    'name': workflow_type['name'],
                    'version': workflow_type['version']
                }
            }
        }))

        return self

    def add_child_workflow(self,
                           workflow,
                           last_state='completed',
                           workflow_id=None,
                           task_list=None,
                           input=None,
                           result=None,
                           control=None):
        self.add_child_workflow_start_initiated(
            workflow,
            workflow_id=workflow_id,
            task_list=task_list,
            input=input,
            control=control)

        if last_state not in CHILD_WORKFLOW_STATES:
            raise ValueError('last_state "{}" not supported for '
                             'a child workflow'.format(last_state))

        if last_state == 'start_initiated':
            return self

        initiated_id = self.last_id
        self.add_child_workflow_started(initiated_id)
        if last_state == 'started':
            return self

        started_id = self.last_id
        if last_state == 'completed':
            self.add_child_workflow_completed(
                initiated_id,
                started_id,
                result=result)
        elif last_state == 'failed':
            self.add_child_workflow_failed(
                initiated_id,
                started_id)
        elif last_state == 'timed_out':
            self.add_child_workflow_timed_out(
                initiated_id,
                started_id,
                'START_TO_CLOSE',
            )
        elif last_state == 'canceled':
            self.add_child_workflow_canceled(
                initiated_id,
                started_id,
            )
        elif last_state == 'terminated':
            self.add_child_workflow_terminated(
                initiated_id,
                started_id,
            )

        return self

    def add_request_cancel(self, cause=None, external_event_id=0):
        self.events.append(EventFactory({
            'eventId': self.next_id,
            'eventTimestamp': new_timestamp_string(),
            'eventType': 'WorkflowExecutionCancelRequested',
            'workflowExecutionCancelRequestedEventAttributes': {
                'externalInitiatedEventId': external_event_id,
                'cause': cause
            }
        }))

        return self

    def add_signal(self, name, input=None, external_event_id=0):
        self.events.append(EventFactory({
            'eventId': self.next_id,
            'eventTimestamp': new_timestamp_string(),
            'eventType': 'WorkflowExecutionSignaled',
            'workflowExecutionSignaledEventAttributes': {
                'externalInitiatedEventId': external_event_id,
                'input': json.dumps(input) if input is not None else '{}',
                'signalName': name,
            }
        }))

        return self
