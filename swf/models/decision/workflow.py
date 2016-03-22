#! -*- coding:utf-8 -*-

# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

import json

from swf.models.workflow import CHILD_POLICIES
from swf.models.decision.base import Decision, decision_action


class WorkflowExecutionDecision(Decision):
    _base_type = 'WorkflowExecution'

    @decision_action
    def complete(self, result=None):
        """Complete workflow execution decision builder

        :param  result: The result of the workflow execution
        :type   result: str
        """
        self.update_attributes({'result': result})

    @decision_action
    def cancel(self, details=None):
        """Cancel workflow execution decision builder

        :param  details: Optional details of the cancellation
        :type   details: str
        """
        self.update_attributes({'details': details})

    @decision_action
    def fail(self, details=None, reason=None):
        """Fail workflow execution decision builder

        :param  details: Optional details of the failure
        :type   details: str

        :param  reason: A descriptive reason for the failure that may help in diagnostics
        :type   reason: str
        """
        self.update_attributes({
            'details': details,
            'reason': reason
        })

    @decision_action
    def terminate(self, reason=None, details=None):
        self.update_attributes({
            'reason': reason,
            'details': details,
        })

    @decision_action
    def continue_as_new(self, child_policy=None,
                        execution_timeout=None, task_timeout=None,
                        input=None, tag_list=None, task_list=None,
                        workflow_type_version=None):
        """Coninue as new workflow execution decision builder
        :param  child_policy: specifies the policy to use for the
                              child workflow executions of the new execution
        :type   child_policy: CHILD_POLICIES.{TERMINATE | REQUEST_CANCEL | ABANDON}

        :param  execution_timeout: specifies the total duration for this workflow execution
        :type   execution_timeout: str

        :param  input: The input provided to the new workflow execution
        :type   input: dict

        :param  tag_list: list of tags to associate with the new workflow execution
        :type   tag_list: list

        :param  task_list: task list name
        :type   task_list: str

        :param  task_timeout: maximum duration of decision tasks for the new workflow execution
        :type   task_timeout: str

        :param  workflow_type_version: workflow type version the execution shold belong to
        :type   workflow_type_version: str
        """
        input = json.dumps(input) or None

        self.update_attributes({
            'childPolicy': child_policy,
            'executionStartToCloseTimeout': execution_timeout,
            'taskStartToCloseTimeout': task_timeout,
            'input': input,
            'tagList': tag_list,
            'taskList': task_list,
            'workflowTypeVersion': workflow_type_version,
        })


class ChildWorkflowExecutionDecision(Decision):
    _base_type = 'ChildWorkflowExecution'

    @decision_action
    def start(self, workflow_type, workflow_id, child_policy=CHILD_POLICIES.TERMINATE,
              execution_timeout='300', task_timeout='300', control=None,
              input=None, tag_list=None, task_list=None):
        """Child workflow execution decision builder

        :param  workflow_type: workflow type to start
        :type   workflow_type: str

        :param  workflow_id: unique id to recognize the workflow execution
        :type   workflow_id: str

        :param  child_policy: specifies the policy to use for the
                              child workflow executions
        :type   child_policy: CHILD_POLICIES.{TERMINATE | REQUEST_CANCEL | ABANDON}

        :param  execution_timeout: specifies the total duration for this workflow execution
        :type   execution_timeout: str

        :param  input: The input provided to the child workflow execution
        :type   input: str

        :param  tag_list: list of tags to associate with the child workflow execution
        :type   tag_list: list

        :param  task_list: task list name
        :type   task_list: str

        :param  task_timeout: maximum duration of decision tasks for the child workflow execution
        :type   task_timeout: str

        """
        input = json.dumps(input) or None

        self.update_attributes({
            'childPolicy': child_policy,
            'executionStartToCloseTimeout': execution_timeout,
            'taskStartToCloseTimeout': task_timeout,
            'control': control,
            'input': input,
            'tagList': tag_list,
            'taskList': {
                'name': task_list,
            },
            'workflowId': workflow_id,
            'workflowType': {
                'name': workflow_type.name,
                'version': workflow_type.version,
            }
        })


class ExternalWorkflowExecutionDecision(Decision):
    _base_type = 'ExternalWorkflowExecution'

    @decision_action
    def request_cancel(self, workflow_id, control=None, run_id=None):
        """External workflow execution decision builder

        :param  workflow_id: id of the external workflow execution to cancel
        :type   workflow_id: str

        :param  : data attached to the event that can be used by
                  the decider in subsequent workflow tasks
        :type   : str

        :param  run_id: run id of the external workflow execution to cancel
        :type   run_id: str
        """
        self.update_attributes({
            'workflowId': workflow_id,
            'control': control,
            'runId': run_id
        })

    @decision_action
    def signal(self, signal_name, workflow_id,
               control=None, input=None, run_id=None):
        """Signal external workflow execution decision builder

        :param  signal_name: name of the signal
        :type   signal_name: str

        :param  workflow_id: workflow id of the workflow execution to be signaled
        :type   workflow_id: str


        :param  control: data attached to the event that can be used by the decider
                         in subsequent decision tasks
        :type   control: str

        :param  input: input to be provided with the signal
        :type   input: str

        :param  run_id: run id of the workflow execution to be signaled
        :type   run_id: str
        """
        input = json.dumps(input) or None

        self.update_attributes({
            'signalName': signal_name,
            'workflowId': workflow_id,
            'control': control,
            'input': input,
            'runId': run_id,
        })
