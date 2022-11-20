from __future__ import annotations

from datetime import datetime

from swf.constants import REGISTERED
from swf.models.workflow import CHILD_POLICIES, WorkflowExecution
from swf.utils import datetime_timestamp


def mock_list_workflow_types(*args, **kwargs):
    override_data = kwargs.pop("override_data", {})

    response = {
        "typeInfos": [
            {
                "creationDate": datetime_timestamp(datetime.now()),
                "deprecationDate": datetime_timestamp(datetime.now()),
                "description": "mocked workflow type",
                "status": REGISTERED,
                "workflowType": {
                    "name": "mocked-workflow type",
                    "version": "0.1",
                },
            }
        ]
    }

    response.update(override_data)

    return response


def mock_describe_workflow_type(*args, **kwargs):
    override_data = kwargs.pop("override_data", {})

    response = {
        "configuration": {
            "defaultChildPolicy": CHILD_POLICIES.TERMINATE,
            "defaultExecutionStartToCloseTimeout": "300",
            "defaultTaskList": {"name": "mocked-tasklist"},
            "defaultTaskStartToCloseTimeout": "300",
        },
        "typeInfo": {
            "creationDate": datetime_timestamp(datetime.now()),
            "deprecationDate": datetime_timestamp(datetime.now()),
            "description": "mocked-workflow-type",
            "status": REGISTERED,
            "workflowType": {"name": "mocked-workflow-type", "version": "0.1"},
        },
    }

    response.update(override_data)

    return response


def mock_list_open_workflow_executions(*args, **kwargs):
    override_data = kwargs.pop("override_data", {})

    response = {
        "executionInfos": [
            {
                "cancelRequested": False,
                "closeStatus": "mocked",
                "closeTimestamp": datetime_timestamp(datetime.now()),
                "execution": {
                    "runId": "mocked-run-id",
                    "workflowId": "mocked-workflow-id",
                },
                "executionStatus": WorkflowExecution.STATUS_OPEN,
                "parent": {
                    "runId": "mocked-parent-run-id",
                    "workflowId": "mocked-parent-workflow-id",
                },
                "startTimestamp": datetime_timestamp(datetime.now()),
                "tagList": ["mocked-tag-1", "mocked-tag-2", "mocked-tag-3"],
                "workflowType": {"name": "mocked-workflow-type", "version": "0.1"},
            }
        ],
    }

    response.update(override_data)

    return response


def mock_list_closed_workflow_executions(*args, **kwargs):
    override_data = kwargs.pop("override_data", {})

    response = {
        "executionInfos": [
            {
                "cancelRequested": False,
                "closeStatus": "mocked",
                "closeTimestamp": datetime_timestamp(datetime.now()),
                "execution": {
                    "runId": "mocked-run-id",
                    "workflowId": "mocked-workflow-id",
                },
                "executionStatus": WorkflowExecution.STATUS_CLOSED,
                "parent": {
                    "runId": "mocked-parent-run-id",
                    "workflowId": "mocked-parent-workflow-id",
                },
                "startTimestamp": datetime_timestamp(datetime.now()),
                "tagList": ["mocked-tag-1", "mocked-tag-2"],
                "workflowType": {"name": "mocked-workflow-type", "version": "0.1"},
            }
        ],
    }

    response.update(override_data)

    return response


def mock_describe_workflow_execution(*args, **kwargs):
    override_data = kwargs.pop("override_data", {})

    response = {
        "executionConfiguration": {
            "childPolicy": CHILD_POLICIES.TERMINATE,
            "executionStartToCloseTimeout": "300",
            "taskList": {"name": "mocked-task-list"},
            "taskStartToCloseTimeout": "300",
        },
        "executionInfo": {
            "cancelRequested": False,
            "closeStatus": WorkflowExecution.STATUS_CLOSED,
            "closeTimestamp": datetime_timestamp(datetime.now()),
            "execution": {"runId": "mocked-run-id", "workflowId": "mocked-workflow-id"},
            "executionStatus": WorkflowExecution.STATUS_OPEN,
            "parent": {"runId": "mocked-run-id", "workflowId": "mocked-workflow-id"},
            "startTimestamp": datetime_timestamp(datetime.now()),
            "tagList": ["mocked-tag-1"],
            "workflowType": {"name": "mocked-workflow-type", "version": "0.1"},
        },
        "latestActivityTaskTimestamp": datetime_timestamp(datetime.now()),
        "latestExecutionContext": "string",
        "openCounts": {
            "openActivityTasks": 12,
            "openChildWorkflowExecutions": 3,
            "openDecisionTasks": 4,
            "openTimers": 5,
        },
    }

    response.update(override_data)

    return response
