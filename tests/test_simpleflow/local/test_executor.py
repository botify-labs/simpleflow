from __future__ import annotations

import unittest

from simpleflow import Workflow
from simpleflow.activity import with_attributes
from simpleflow.canvas import Chain, Group
from simpleflow.constants import HOUR, MINUTE
from simpleflow.local import Executor
from simpleflow.task import WorkflowTask


@with_attributes()
def to_int(arg):
    return int(arg)


class MyWorkflow(Workflow):
    name = "test_workflow"
    version = "test_version"
    task_list = "test_task_list"
    decision_tasks_timeout = 5 * MINUTE
    execution_timeout = 1 * HOUR


executor = Executor(MyWorkflow)
executor.initialize_history({})
executor._workflow = MyWorkflow(executor)


class TestRunContext(unittest.TestCase):
    def test_run_context_for_child_workflows(self):
        """Test that the local executor adds a different run_id for each
        child workflows.
        """

        class ChildWorkflow3(Workflow):
            name = "ChildWorkflow3"

            @classmethod
            def get_workflow_id(*args, **kwargs):
                return kwargs["name"]

            def run(self, name, *args, **kwargs):
                return {
                    "run_id": self.get_run_context()["run_id"],
                    "workflow_id": self.get_run_context()["workflow_id"],
                }

        class ChildWorkflow2(Workflow):
            name = "ChildWorkflow2"

            def run(self, *args, **kwargs):
                return {
                    "run_id": self.get_run_context()["run_id"],
                    "workflow_id": self.get_run_context()["workflow_id"],
                }

        class ChildWorkflow1(Workflow):
            name = "ChildWorkflow"

            def run(self, *args, **kwargs):
                results = Chain(ChildWorkflow2, (to_int, "2")).submit(self.executor).result
                return {
                    "workflow_id": self.get_run_context()["workflow_id"],
                    "run_id": self.get_run_context()["run_id"],
                    "child": results,
                }

        result = (
            Group(
                ChildWorkflow1,
                ChildWorkflow1,
                WorkflowTask(None, ChildWorkflow3, name="test_workflow_id"),
            )
            .submit(executor)
            .result
        )
        child1_1, child1_2, child3 = result
        self.assertTrue(child1_1["workflow_id"].startswith("local_childworkflow"))
        self.assertTrue(child1_2["workflow_id"].startswith("local_childworkflow"))
        self.assertNotEqual(child1_1["run_id"], child1_2["run_id"])

        child2_1 = child1_1["child"][0]
        child2_2 = child1_2["child"][0]
        self.assertTrue(child2_1["workflow_id"].startswith("local_childworkflow2"))
        self.assertTrue(child2_2["workflow_id"].startswith("local_childworkflow2"))
        self.assertNotEqual(child2_1["run_id"], child2_2["run_id"])

        self.assertEqual(child3["workflow_id"], "test_workflow_id")
        self.assertEqual(child1_1["workflow_id"], "local_childworkflow")
