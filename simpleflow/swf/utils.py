from __future__ import absolute_import

import swf.exceptions
import swf.models
import swf.querysets
from simpleflow.history import History


if False:
    from typing import Any, List, Dict  # NOQA
    from swf.models.decision.base import Decision  # NOQA


# TODO: move this function inside a QuerySet object when we merge the
# "simpleflow" and "swf" namespaces
def get_workflow_execution(domain_name, workflow_id, run_id=None):
    domain = swf.models.Domain(domain_name)

    # if no run_id provided, we assume that the requester wanted the last
    # execution with that workflow_id
    found_run_id = None
    if not run_id:
        qs = swf.querysets.WorkflowExecutionQuerySet(domain)
        wfe = (qs.filter(workflow_id=workflow_id, status=swf.models.WorkflowExecution.STATUS_OPEN) or
               qs.filter(workflow_id=workflow_id, status=swf.models.WorkflowExecution.STATUS_CLOSED))
        if wfe:
            # by default, workflow executions are returned in descending start time order
            # so the first returned is the last that has run
            found_run_id = wfe[0].run_id
        else:
            # we would send a malformed request to SWF API, better stop directly
            raise ValueError("Couldn't find an execution with workflowId={}".format(workflow_id))

    return swf.querysets.WorkflowExecutionQuerySet(domain).get(
        workflow_id=workflow_id,
        run_id=run_id or found_run_id,
    )


# TODO: move this function inside a QuerySet object when we merge the
# "simpleflow" and "swf" namespaces
def get_workflow_history(domain_name, workflow_id, run_id=None):
    workflow_execution = get_workflow_execution(domain_name, workflow_id, run_id=run_id)
    history = History(workflow_execution.history())
    return history


def sanitize_activity_context(context):
    return {
        "name": context["activityType"]["name"],
        "version": context["activityType"]["version"],
        "workflow_id": context["workflowExecution"]["workflowId"],
        "run_id": context["workflowExecution"]["runId"],
        "activity_id": context["activityId"],
        "input": context["input"]
    }


class DecisionsAndContext(object):
    """
    Encapsulate decisions and execution context.
    The execution context contains keys with either plain values, lists or sets.
    """
    def __init__(self, decisions=None, execution_context=None):
        self.decisions = decisions or []  # type: List[Decision]
        self.execution_context = execution_context  # type: Dict[str, Any]

    def __repr__(self):
        return '<{} decisions={}, execution_context={}>'.format(
            self.__class__.__name__, self.decisions, self.execution_context
        )

    def append_decision(self, decision):
        # type: (Decision) -> None
        """
        Append a decision.
        """
        self.decisions.append(decision)

    def extend_decision(self, decisions):
        # type: (List[Decision]) -> None
        """
        Append a list of decisions.
        """
        self.decisions += decisions

    def append_kv_to_context(self, key, value):
        # type: (str, Any) -> None
        """
        Set a (key, value) in the execution context.
        """
        if self.execution_context is None:
            self.execution_context = {}
        self.execution_context[key] = value

    def append_kv_to_list_context(self, key, value):
        # type: (str, Any) -> None
        """
        Append a value to a list in the execution context.
        """
        if self.execution_context is None:
            self.execution_context = {}
        if key not in self.execution_context:
            self.execution_context[key] = []
        self.execution_context[key].append(value)

    def append_kv_to_set_context(self, key, value):
        # type: (str, Any) -> None
        """
        Add a value to a set in the execution context.
        """
        if self.execution_context is None:
            self.execution_context = {}
        if key not in self.execution_context:
            self.execution_context[key] = set()
        self.execution_context[key].add(value)
