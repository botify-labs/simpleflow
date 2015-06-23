from __future__ import absolute_import

import swf.models
import swf.querysets
import swf.exceptions

from . import pretty


def show_workflow_stats(domain_name, workflow_id, run_id, nb_tasks):
    domain = swf.models.Domain(domain_name)
    try:
        workflow_execution = swf.querysets.WorkflowExecutionQuerySet(domain).get(
            workflow_id=workflow_id,
            run_id=run_id,
        )
    except swf.exceptions.DoesNotExistError:
        workflow_execution = swf.querysets.WorkflowExecutionQuerySet(domain).get(
            workflow_id=workflow_id,
            run_id=run_id,
            workflow_status=swf.models.WorkflowExecution.STATUS_CLOSED,
        )

    return pretty.show(workflow_execution, nb_tasks)
