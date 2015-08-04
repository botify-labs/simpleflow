from __future__ import absolute_import
import swf.models
import swf.querysets

from simpleflow.history import History


def get_workflow_history(domain_name, workflow_id, run_id):
    domain = swf.models.Domain(domain_name)
    workflow_execution = (
        swf.querysets.WorkflowExecutionQuerySet(domain).get(
            workflow_id=workflow_id,
            run_id=run_id,
        )
    )

    return History(workflow_execution.history())
