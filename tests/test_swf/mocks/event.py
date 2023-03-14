from __future__ import annotations


def mock_get_workflow_execution_history(*args, **kwargs):
    override_data = kwargs.pop("override_data", {})

    response = {
        "events": [
            {
                "eventId": 1,
                "eventType": "WorkflowExecutionStarted",
                "workflowExecutionStartedEventAttributes": {
                    "taskList": {"name": "test"},
                    "parentInitiatedEventId": 0,
                    "taskStartToCloseTimeout": "300",
                    "childPolicy": "TERMINATE",
                    "executionStartToCloseTimeout": "6000",
                    "workflowType": {"version": "0.1", "name": "test-crawl-fsm1"},
                },
                "eventTimestamp": 1365177769.585,
            },
            {
                "eventId": 2,
                "eventType": "DecisionTaskScheduled",
                "decisionTaskScheduledEventAttributes": {
                    "startToCloseTimeout": "300",
                    "taskList": {"name": "test"},
                },
                "eventTimestamp": 1365177769.585,
            },
        ]
    }

    response.update(override_data)

    return response
