from __future__ import absolute_import

import swf.exceptions
import swf.models
import swf.querysets
from simpleflow.history import History


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
    """
    Get workflow history.
    :param domain_name:
    :type domain_name: str
    :param workflow_id:
    :type workflow_id: str
    :param run_id:
    :type run_id: str
    :return:
    :rtype: History
    """
    workflow_execution = get_workflow_execution(domain_name, workflow_id, run_id=run_id)
    return History(workflow_execution.history())
