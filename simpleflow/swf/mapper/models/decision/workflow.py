from __future__ import annotations

from typing import Any

from simpleflow import format
from simpleflow.swf.mapper.models.decision.base import Decision, decision_action
from simpleflow.swf.mapper.models.workflow import CHILD_POLICIES, WorkflowType


class WorkflowExecutionDecision(Decision):
    _base_type = "WorkflowExecution"

    @decision_action
    def complete(self, result: dict[str, Any] | None = None) -> None:
        """Complete workflow execution decision builder

        :param  result: The result of the workflow execution
        """
        self.update_attributes(
            {
                "result": format.result(result),
            }
        )

    @decision_action
    def cancel(self, details: dict[str, Any] | None = None) -> None:
        """Cancel workflow execution decision builder

        :param  details: Optional details of the cancellation
        """
        self.update_attributes(
            {
                "details": format.details(details),
            }
        )

    @decision_action
    def fail(self, details: Any = None, reason: Any = None) -> None:
        """Fail workflow execution decision builder

        :param  details: Optional details of the failure
        :param  reason: A descriptive reason for the failure that may help in diagnostics
        """
        self.update_attributes(
            {
                "details": format.details(details),
                "reason": format.reason(reason),
            }
        )

    @decision_action
    def terminate(self, reason: Any = None, details: Any = None) -> None:
        self.update_attributes(
            {
                "reason": format.reason(reason),
                "details": format.details(details),
            }
        )

    @decision_action
    def continue_as_new(
        self,
        child_policy: CHILD_POLICIES | None = None,
        execution_timeout: str | None = None,
        task_timeout: str | None = None,
        input: dict[str, Any] | None = None,
        tag_list: list[str] | None = None,
        task_list: str | None = None,
        task_priority: str | None = None,
        workflow_type_version: str | None = None,
    ):
        """Continue as new workflow execution decision builder
        :param  child_policy: specifies the policy to use for the
                              child workflow executions of the new execution
        :param  execution_timeout: specifies the total duration for this workflow execution
        :param  input: The input provided to the new workflow execution
        :param  tag_list: list of tags to associate with the new workflow execution
        :param  task_list: task list name
        :param  task_priority:
        :param  task_timeout: maximum duration of decision tasks for the new workflow execution
        :param  workflow_type_version: workflow type version the execution should belong to
        """
        if input is not None:
            input = format.input(input)

        self.update_attributes(
            {
                "childPolicy": child_policy,
                "executionStartToCloseTimeout": execution_timeout,
                "taskStartToCloseTimeout": task_timeout,
                "input": input,
                "tagList": tag_list,
                "taskList": {
                    "name": task_list,
                },
                "taskPriority": task_priority,
                "workflowTypeVersion": workflow_type_version,
            }
        )


class ChildWorkflowExecutionDecision(Decision):
    _base_type = "ChildWorkflowExecution"

    @decision_action
    def start(
        self,
        workflow_type: WorkflowType,
        workflow_id: str,
        child_policy: CHILD_POLICIES = CHILD_POLICIES.TERMINATE,
        execution_timeout: str = "300",
        task_timeout: str = "300",
        control: dict[str, Any] | None = None,
        input: dict[str, Any] | None = None,
        tag_list: list[str] | None = None,
        task_list: str | None = None,
    ) -> None:
        """Child workflow execution decision builder

        :param  workflow_type: workflow type to start
        :param  workflow_id: unique id to recognize the workflow execution
        :param  child_policy: specifies the policy to use for the
                              child workflow executions
        :param  execution_timeout: specifies the total duration for this workflow execution
        :param  input: The input provided to the child workflow execution
        :param  tag_list: list of tags to associate with the child workflow execution
        :param  task_list: task list name
        :param  task_timeout: maximum duration of decision tasks for the child workflow execution
        :param  control: data attached to the event that can be used by
                  the decider in subsequent workflow tasks
        """
        if input is not None:
            input = format.input(input)
        if control is not None:
            control = format.control(control)

        self.update_attributes(
            {
                "childPolicy": child_policy,
                "executionStartToCloseTimeout": execution_timeout,
                "taskStartToCloseTimeout": task_timeout,
                "control": control,
                "input": input,
                "tagList": tag_list,
                "taskList": {
                    "name": task_list,
                },
                "workflowId": workflow_id,
                "workflowType": {
                    "name": workflow_type.name,
                    "version": workflow_type.version,
                },
            }
        )


class ExternalWorkflowExecutionDecision(Decision):
    _base_type = "ExternalWorkflowExecution"

    @decision_action
    def request_cancel(
        self, workflow_id: str, control: dict[str, Any] | None = None, run_id: str | None = None
    ) -> None:
        """External workflow execution decision builder

        :param  workflow_id: id of the external workflow execution to cancel
        :param  control: data attached to the event that can be used by
                  the decider in subsequent workflow tasks
        :param  run_id: run id of the external workflow execution to cancel
        """
        if control is not None:
            control = format.control(control)

        self.update_attributes({"workflowId": workflow_id, "control": control, "runId": run_id})

    @decision_action
    def signal(
        self,
        signal_name: str,
        workflow_id: str,
        control: dict[str, Any] | None = None,
        input: dict[str, Any] | None = None,
        run_id: str | None = None,
    ) -> None:
        """Signal external workflow execution decision builder

        :param  signal_name: name of the signal
        :param  workflow_id: workflow id of the workflow execution to be signaled
        :param  control: data attached to the event that can be used by the decider
                         in subsequent decision tasks
        :param  input: input to be provided with the signal
        :param  run_id: run id of the workflow execution to be signaled
        """
        if input is not None:
            input = format.input(input)
        if control is not None:
            control = format.control(control)

        self.update_attributes(
            {
                "signalName": signal_name,
                "workflowId": workflow_id,
                "control": control,
                "input": input,
                "runId": run_id,
            }
        )
