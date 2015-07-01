from __future__ import absolute_import

import swf.models
import swf.querysets
import swf.exceptions

from . import pretty


__all__ = [
    'show_workflow_profile',
    'show_workflow_status',
    'list_workflow_executions',
]


def get_workflow_execution(domain_name, workflow_id, run_id=None):
    def filter_execution(*args, **kwargs):
        if 'workflow_status' in kwargs:
            kwargs['status'] = kwargs.pop('workflow_status')
        return query.filter(*args, **kwargs)[0]

    domain = swf.models.Domain(domain_name)
    query = swf.querysets.WorkflowExecutionQuerySet(domain)

    action = filter_execution
    keywords = {
        'workflow_id': workflow_id,
    }
    if run_id:
        action = query.get
        keywords['run_id'] = run_id

    try:
        workflow_execution = action(**keywords)
    except (swf.exceptions.DoesNotExistError, IndexError):
        keywords['workflow_status'] = swf.models.WorkflowExecution.STATUS_CLOSED
        workflow_execution = action(**keywords)

    return workflow_execution


def show_workflow_info(domain_name, workflow_id, run_id=None):
    workflow_execution = get_workflow_execution(
        domain_name,
        workflow_id,
        run_id,
    )
    return pretty.info(workflow_execution)


def show_workflow_profile(domain_name, workflow_id, run_id=None, nb_tasks=None):
    workflow_execution = get_workflow_execution(
        domain_name,
        workflow_id,
        run_id,
    )
    return pretty.profile(workflow_execution, nb_tasks)


def show_workflow_status(domain_name, workflow_id, run_id=None, nb_tasks=None):
    workflow_execution = get_workflow_execution(
        domain_name,
        workflow_id,
        run_id,
    )
    return pretty.status(workflow_execution, nb_tasks)


def list_workflow_executions(domain_name):
    domain = swf.models.Domain(domain_name)
    query = swf.querysets.WorkflowExecutionQuerySet(domain)
    executions = query.all()

    return pretty.list(executions)
