from __future__ import annotations

from simpleflow.constants import HOUR, MINUTE
from simpleflow.workflow import Workflow


class BaseTestWorkflow(Workflow):
    name = "test_workflow"
    version = "test_version"
    task_list = "test_task_list"
    decision_tasks_timeout = 5 * MINUTE
    execution_timeout = 1 * HOUR
