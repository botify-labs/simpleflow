from __future__ import absolute_import

import swf.models
import swf.querysets

from . import pretty


def show_workflow_stats(domain_name, workflow_id, run_id):
    domain = swf.models.Domain(domain_name)
    workflow_execution = (
        swf.querysets.WorkflowExecutionQuerySet(domain).get(
            workflow_id=workflow_id,
            run_id=run_id,
        )
    )

    return pretty.show(workflow_execution)
