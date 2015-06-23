from __future__ import absolute_import

import swf.models
import swf.querysets
import swf.exceptions

from . import pretty


__all__ = ['show_workflow_stats', 'show_workflow_status']


def get_workflow_execution(domain_name, workflow_id, run_id):
    domain = swf.models.Domain(domain_name)
    query = swf.querysets.WorkflowExecutionQuerySet(domain)
    try:
        workflow_execution = query.get(
            workflow_id=workflow_id,
            run_id=run_id,
        )
    except swf.exceptions.DoesNotExistError:
        workflow_execution = query.get(
            workflow_id=workflow_id,
            run_id=run_id,
            workflow_status=swf.models.WorkflowExecution.STATUS_CLOSED,
        )
    return workflow_execution


def show_workflow_stats(domain_name, workflow_id, run_id, nb_tasks):
    workflow_execution = get_workflow_execution(
        domain_name,
        workflow_id,
        run_id,
    )
    return pretty.show(workflow_execution, nb_tasks)


def show_workflow_status(domain_name, workflow_id, run_id, nb_tasks):
    workflow_execution = get_workflow_execution(
        domain_name,
        workflow_id,
        run_id,
    )
    return pretty.status(workflow_execution, nb_tasks)
