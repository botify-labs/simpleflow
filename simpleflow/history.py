from __future__ import annotations

import collections
from typing import TYPE_CHECKING, Callable

import swf.models.history
from simpleflow import logger

if TYPE_CHECKING:
    from typing import Any

    from swf.models.event import ActivityTaskEvent, Event
    from swf.models.event.task import ActivityTaskEventDict


class History:
    """
    History data.
    """

    def __init__(self, history):
        self._history: swf.models.history.History = history
        self._activities: dict[int, ActivityTaskEventDict] = {}
        self._child_workflows = {}
        self._external_workflows_signaling = {}
        self._external_workflows_canceling = {}
        self._signals = {}
        self._signaled_workflows = collections.defaultdict(list)
        self._markers = {}
        self._timers = {}
        self._tasks = []
        self._cancel_requested = None
        self._cancel_failed = None
        self.started_decision_id = None
        self.completed_decision_id = None

    @property
    def swf_history(self):
        """

        :return: SWF history
        :rtype: swf.models.history.History
        """
        return self._history

    @property
    def activities(self) -> dict[int, ActivityTaskEventDict]:
        """
        :return: activities
        """
        return self._activities

    @property
    def child_workflows(self):
        """
        :return: child WFs
        :rtype: collections.OrderedDict[str, dict[str, Any]]
        """
        return self._child_workflows

    @property
    def external_workflows_signaling(self):
        """
        :return: external WFs
        :rtype: collections.OrderedDict[str, dict[str, Any]]
        """
        return self._external_workflows_signaling

    @property
    def signals(self):
        """
        :return: signals
        :rtype: collections.OrderedDict[str, dict[str, Any]]
        """
        return self._signals

    @property
    def cancel_requested(self):
        """
        :return: Last cancel requested event, if any.
        :rtype: Optional[dict]
        """
        return self._cancel_requested

    @property
    def cancel_failed(self):
        """
        :return: Last cancel failed event, if any.
        :rtype: Optional[dict]
        """
        return self._cancel_failed

    @property
    def cancel_requested_id(self):
        """
        :return: ID of last cancel requested event, if any.
        :rtype: Optional[int]
        """
        return self._cancel_requested["event_id"] if self._cancel_requested else None

    @property
    def cancel_failed_decision_task_completed_event_id(self):
        """
        :return: ID of last cancel failed event, if any.
        :rtype: Optional[int]
        """
        return self._cancel_failed["decision_task_completed_event_id"] if self._cancel_failed else None

    @property
    def signaled_workflows(self):
        """
        :return: signaled workflows
        :rtype: defaultdict(list)
        """
        return self._signaled_workflows

    @property
    def markers(self):
        """

        :return: Markers
        :rtype: collections.OrderedDict[str, list[dict[str, Any]]]
        """
        return self._markers

    @property
    def timers(self) -> dict[str, dict[str, Any]]:
        return self._timers

    @property
    def tasks(self):
        """
        :return:
         :rtype: list[dict[str, Any]]
        """
        return self._tasks

    @property
    def events(self) -> list[swf.models.event.Event]:
        return self._history.events

    def parse_activity_event(self, events: list[ActivityTaskEvent], event: ActivityTaskEvent):
        """
        Aggregate all the attributes of an activity in a single entry.
        """

        def get_activity():
            """
            Return a reference to the corresponding activity.
            :return: mutable activity
            :rtype: dict[str, Any]
            """
            scheduled_event = events[event.scheduled_event_id - 1]
            return self._activities[scheduled_event.activity_id]

        if event.state == "scheduled":
            activity = {
                "type": "activity",
                "id": event.activity_id,
                "name": event.activity_type["name"],
                "version": event.activity_type["version"],
                "state": event.state,
                "scheduled_id": event.id,
                "scheduled_timestamp": event.timestamp,
                "input": event.input,
                "task_list": event.task_list["name"],
                "control": event.control,
                "decision_task_completed_event_id": event.decision_task_completed_event_id,
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
        elif event.state == "schedule_failed":
            activity = {
                "type": "activity",
                "state": event.state,
                "cause": event.cause,
                "activity_type": event.activity_type.copy(),
                "schedule_failed_timestamp": event.timestamp,
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

        elif event.state == "started":
            activity = get_activity()
            activity.update(
                {
                    "state": event.state,
                    "identity": event.identity,
                    "started_id": event.id,
                    "started_timestamp": event.timestamp,
                }
            )
        elif event.state == "completed":
            activity = get_activity()
            activity.update(
                {
                    "state": event.state,
                    "result": getattr(event, "result", None),
                    "completed_id": event.id,
                    "completed_timestamp": event.timestamp,
                }
            )
        elif event.state == "timed_out":
            activity = get_activity()
            activity.update(
                {
                    "state": event.state,
                    "timeout_type": event.timeout_type,
                    "timeout_value": getattr(
                        events[activity["scheduled_id"] - 1],
                        f"{event.timeout_type.lower()}_timeout",
                    ),
                    "timed_out_id": event.id,
                    "timed_out_timestamp": event.timestamp,
                }
            )
            if "retry" not in activity:
                activity["retry"] = 0
            else:
                activity["retry"] += 1
        elif event.state == "failed":
            activity = get_activity()
            activity.update(
                {
                    "state": event.state,
                    "reason": getattr(event, "reason", ""),
                    "details": getattr(event, "details", ""),
                    "failed_id": event.id,
                    "failed_timestamp": event.timestamp,
                }
            )
            if "retry" not in activity:
                activity["retry"] = 0
            else:
                activity["retry"] += 1
        elif event.state == "cancelled":
            activity = get_activity()
            activity.update(
                {
                    "state": event.state,
                    "details": getattr(event, "details", ""),
                    "cancelled_timestamp": event.timestamp,
                }
            )
        elif event.state == "cancel_requested":
            activity = {
                "type": "activity",
                "id": event.activity_id,
                "state": event.state,
                "cancel_requested_timestamp": event.timestamp,
                "cancel_decision_task_completed_event_id": event.decision_task_completed_event_id,
            }
            if event.activity_id not in self._activities:
                self._activities[event.activity_id] = activity
                self._tasks.append(activity)
            else:
                self._activities[event.activity_id].update(activity)

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

        if event.state == "start_initiated":
            workflow = {
                "type": "child_workflow",
                "id": event.workflow_id,
                "name": event.workflow_type["name"],
                "version": event.workflow_type["version"],
                "state": event.state,
                "initiated_event_id": event.id,
                "raw_input": event.raw.get("input"),  # FIXME obsolete; any user out there?
                "input": event.input,
                "child_policy": event.child_policy,
                "control": event.control,
                "tag_list": getattr(event, "tag_list", None),
                "task_list": event.task_list["name"],
                "initiated_event_timestamp": event.timestamp,
                "decision_task_completed_event_id": event.decision_task_completed_event_id,
            }
            if event.workflow_id not in self._child_workflows:
                self._child_workflows[event.workflow_id] = workflow
                self._tasks.append(workflow)
            else:
                # May have gotten a start_failed before (or retrying?)
                if self._child_workflows[event.workflow_id]["state"] == "start_initiated":
                    # Should not happen anymore
                    logger.warning(
                        "start_initiated again for workflow {} (initiated @{}, we're @{})".format(
                            event.workflow_id,
                            self._child_workflows[event.workflow_id]["initiated_event_id"],
                            event.id,
                        )
                    )
                self._child_workflows[event.workflow_id].update(workflow)
        elif event.state == "start_failed":
            workflow = {
                "type": "child_workflow",
                "id": event.workflow_id,
                "state": event.state,
                "cause": event.cause,
                "name": event.workflow_type["name"],
                "version": event.workflow_type["version"],
                "control": event.control,
                "start_failed_id": event.id,
                "start_failed_timestamp": event.timestamp,
                "decision_task_completed_event_id": event.decision_task_completed_event_id,
            }
            if event.workflow_id not in self._child_workflows:
                self._child_workflows[event.workflow_id] = workflow
                self._tasks.append(workflow)
            else:
                self._child_workflows[event.workflow_id].update(workflow)
        elif event.state == "started":
            workflow = get_workflow()
            workflow.update(
                {
                    "state": event.state,
                    "run_id": event.workflow_execution["runId"],
                    "workflow_id": event.workflow_execution["workflowId"],
                    "started_id": event.id,
                    "started_timestamp": event.timestamp,
                }
            )
        elif event.state == "completed":
            workflow = get_workflow()
            workflow.update(
                {
                    "state": event.state,
                    "result": getattr(event, "result", None),
                    "completed_id": event.id,
                    "completed_timestamp": event.timestamp,
                }
            )
        elif event.state == "failed":
            workflow = get_workflow()
            workflow.update(
                {
                    "state": event.state,
                    "reason": getattr(event, "reason", None),
                    "details": getattr(event, "details", None),
                    "failed_id": event.id,
                    "failed_timestamp": event.timestamp,
                }
            )
            if "retry" not in workflow:
                workflow["retry"] = 0
            else:
                workflow["retry"] += 1
        elif event.state == "timed_out":
            workflow = get_workflow()
            workflow.update(
                {
                    "state": event.state,
                    "timeout_type": event.timeout_type,
                    "timeout_value": getattr(
                        events[workflow["initiated_event_id"] - 1],
                        f"{event.timeout_type.lower()}_timeout",
                        None,
                    ),
                    "timed_out_id": event.id,
                    "timed_out_timestamp": event.timestamp,
                }
            )
            if "retry" not in workflow:
                workflow["retry"] = 0
            else:
                workflow["retry"] += 1
        elif event.state == "canceled":
            workflow = get_workflow()
            workflow.update(
                {
                    "state": event.state,
                    "details": getattr(event, "details", None),
                    "canceled_id": event.id,
                    "canceled_timestamp": event.timestamp,
                }
            )
        elif event.state == "terminated":
            workflow = get_workflow()
            workflow.update(
                {
                    "state": event.state,
                    "terminated_id": event.id,
                    "terminated_timestamp": event.timestamp,
                }
            )

    def parse_workflow_event(self, events, event):
        """
        Parse a workflow event.
        :param events:
        :param event:
        """
        if event.state == "signaled":
            signal = {
                "type": "signal",
                "name": event.signal_name,
                "state": event.state,
                "external_initiated_event_id": getattr(event, "external_initiated_event_id", None),
                "external_run_id": getattr(event, "external_workflow_execution", {}).get("runId"),
                "external_workflow_id": getattr(event, "external_workflow_execution", {}).get("workflowId"),
                "input": event.input,
                "event_id": event.id,
                "timestamp": event.timestamp,
            }
            self._signals[event.signal_name] = signal
            self._tasks.append(signal)
        elif event.state == "cancel_requested":
            cancel_requested = {
                "type": event.state,
                "cause": getattr(event, "cause", None),
                "external_initiated_event_id": getattr(event, "external_initiated_event_id", None),
                "external_run_id": getattr(event, "external_workflow_execution", {}).get("runId"),
                "external_workflow_id": getattr(event, "external_workflow_execution", {}).get("workflowId"),
                "event_id": event.id,
                "timestamp": event.timestamp,
            }
            self._cancel_requested = cancel_requested
        elif event.state == "cancel_failed":
            cancel_failed = {
                "type": event.state,
                "cause": getattr(event, "cause", None),
                "event_id": event.id,
                "decision_task_completed_event_id": event.decision_task_completed_event_id,
                "timestamp": event.timestamp,
            }
            self._cancel_failed = cancel_failed

    def parse_external_workflow_event(self, events, event):
        """
        Parse an external workflow event.
        :param events:
        :param event:
        """

        def get_workflow(workflows):
            initiated_event = events[event.initiated_event_id - 1]
            return workflows[initiated_event.workflow_id]

        if event.state == "signal_execution_initiated":
            workflow = {
                "type": "external_workflow",
                "id": event.workflow_id,
                "run_id": getattr(event, "run_id", None),
                "signal_name": event.signal_name,
                "state": event.state,
                "initiated_event_id": event.id,
                "input": event.input,
                "control": event.control,
                "initiated_event_timestamp": event.timestamp,
            }
            self._external_workflows_signaling[event.id] = workflow
        elif event.state == "signal_execution_failed":
            workflow = self._external_workflows_signaling[event.initiated_event_id]
            workflow.update(
                {
                    "state": event.state,
                    "cause": event.cause,
                    "signal_failed_timestamp": event.timestamp,
                }
            )
            if event.control:
                workflow["control"] = event.control
        elif event.state == "execution_signaled":
            workflow = self._external_workflows_signaling[event.initiated_event_id]
            workflow.update(
                {
                    "state": event.state,
                    "run_id": event.workflow_execution["runId"],
                    "workflow_id": event.workflow_execution["workflowId"],
                    "signaled_event_id": event.id,
                    "signaled_timestamp": event.timestamp,
                }
            )
            self._signaled_workflows[workflow["signal_name"]].append(workflow)
        elif event.state == "request_cancel_execution_initiated":
            workflow = {
                "type": "external_workflow",
                "id": event.workflow_id,
                "run_id": getattr(event, "run_id", None),
                "state": event.state,
                "control": event.control,
                "initiated_event_id": event.id,
                "initiated_event_timestamp": event.timestamp,
            }
            if event.workflow_id not in self._external_workflows_canceling:
                self._external_workflows_canceling[event.workflow_id] = workflow
            else:
                logger.warning(
                    "request_cancel_initiated again for workflow {} (initiated @{}, we're @{})".format(
                        event.workflow_id,
                        self._external_workflows_canceling[event.workflow_id]["initiated_event_id"],
                        event.id,
                    )
                )
                self._external_workflows_canceling[event.workflow_id].update(workflow)
        elif event.state == "request_cancel_execution_failed":
            workflow = get_workflow(self._external_workflows_canceling)
            workflow.update(
                {
                    "state": event.state,
                    "cause": event.cause,
                }
            )
            if event.control:
                workflow["control"] = event.control
            workflow["request_cancel_failed_timestamp"] = event.timestamp
        elif event.state == "execution_cancel_requested":
            workflow = get_workflow(self._external_workflows_canceling)
            workflow.update(
                {
                    "run_id": event.workflow_execution["runId"],
                    "workflow_id": event.workflow_execution["workflowId"],
                    "cancel_requested_event_id": event.id,
                    "cancel_requested_timestamp": event.timestamp,
                }
            )

    def parse_marker_event(self, events, event):
        if event.state == "recorded":
            marker = {
                "type": "marker",
                "name": event.marker_name,
                "state": event.state,
                "details": getattr(event, "details", None),
                "event_id": event.id,
                "timestamp": event.timestamp,
            }
            self._markers.setdefault(event.marker_name, []).append(marker)
        elif event.state == "record_failed":
            marker = {
                "type": "marker",
                "name": event.marker_name,
                "state": event.state,
                "cause": event.cause,
                "record_failed_event_id": event.id,
                "record_failed_event_timestamp": event.timestamp,
            }
            self._markers.setdefault(event.marker_name, []).append(marker)

    def parse_timer_event(self, events, event):
        if event.state == "started":
            timer = {
                "type": "timer",
                "id": event.timer_id,
                "state": event.state,
                "start_to_fire_timeout": int(event.start_to_fire_timeout),
                "control": event.control,
                "started_event_id": event.id,
                "started_event_timestamp": event.timestamp,
                "decision_task_completed_event_id": event.decision_task_completed_event_id,
            }
            self._timers[event.timer_id] = timer
        elif event.state == "fired":
            timer = self._timers[event.timer_id]
            timer.update(
                {
                    "state": event.state,
                    "fired_event_id": event.id,
                    "fired_event_timestamp": event.timestamp,
                }
            )
        elif event.state == "start_failed":
            timer = self._timers.get(event.timer_id)
            if timer is None:
                timer = {
                    "type": "timer",
                    "id": event.timer_id,
                    "decision_task_completed_event_id": event.decision_task_completed_event_id,
                }
                self._timers[event.timer_id] = timer
            timer.update(
                {
                    "state": event.state,
                    "cause": event.cause,
                    "start_failed_event_id": event.id,
                    "start_failed_event_timestamp": event.timestamp,
                }
            )
        elif event.state == "canceled":
            timer = self._timers[event.timer_id]
            timer.update(
                {
                    "state": event.state,
                    "canceled_event_id": event.id,
                    "canceled_event_timestamp": event.timestamp,
                    "cancel_decision_task_completed_event_id": event.decision_task_completed_event_id,
                }
            )
        elif event.state == "cancel_failed":
            timer = self._timers.get(event.timer_id)
            if timer is None:
                timer = {
                    "type": "timer",
                    "id": event.timer_id,
                    "cancel_decision_task_completed_event_id": event.decision_task_completed_event_id,
                }
                self._timers[event.timer_id] = timer
            timer.update(
                {
                    "state": event.state,
                    "cancel_failed_event_id": event.id,
                    "cancel_failed_event_timestamp": event.timestamp,
                }
            )

    def parse_decision_event(self, events, event):
        if event.state == "started":
            self.started_decision_id = event.id
        if event.state == "completed":
            self.completed_decision_id = event.id

    TYPE_TO_PARSER: dict[str, Callable[[History, list[Event], Event], None]] = {
        "ActivityTask": parse_activity_event,
        "DecisionTask": parse_decision_event,
        "ChildWorkflowExecution": parse_child_workflow_event,
        "WorkflowExecution": parse_workflow_event,
        "ExternalWorkflowExecution": parse_external_workflow_event,
        "Marker": parse_marker_event,
        "Timer": parse_timer_event,
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

    @staticmethod
    def get_event_id(event: dict[str, Any]) -> int | None:
        for event_id_key in (  # FIXME add a universal name?..
            "scheduled_id",
            "initiated_event_id",
            "event_id",
            "started_event_id",
            "cancel_failed_event_id",
        ):
            event_id = event.get(event_id_key)
            if event_id:
                return event_id
