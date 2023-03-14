from __future__ import annotations

import unittest
from unittest.mock import patch

from boto.exception import SWFResponseError
from boto.swf.exceptions import SWFTypeAlreadyExistsError
from boto.swf.layer1 import Layer1

from swf.exceptions import AlreadyExistsError, DoesNotExistError, ResponseError
from swf.models.domain import Domain
from swf.models.history import History
from swf.models.workflow import WorkflowExecution, WorkflowType

from ..mocks.event import mock_get_workflow_execution_history
from ..mocks.workflow import (
    mock_describe_workflow_execution,
    mock_describe_workflow_type,
)


class CustomAssertions:
    def assertLength(self, a_list, count):
        self.assertEqual(
            len(a_list),
            count,
            "Expected length to be {}, got {}. Object: {}".format(count, len(a_list), a_list),
        )


class TestWorkflowType(unittest.TestCase, CustomAssertions):
    def setUp(self):
        self.domain = Domain("test-domain")
        self.wt = WorkflowType(self.domain, "TestType", "1.0")

    def tearDown(self):
        pass

    def test_init_with_invalid_child_policy(self):
        with self.assertRaises(ValueError):
            WorkflowType(self.domain, "TestType", "1.0", child_policy="FAILING_POLICY")

    def test___diff_with_different_workflow_type(self):
        with patch.object(
            Layer1,
            "describe_workflow_type",
            mock_describe_workflow_type,
        ):
            workflow_type = WorkflowType(self.domain, "different-workflow-type", version="different-version")
            diffs = workflow_type._diff()

            self.assertIsNotNone(diffs)
            self.assertLength(diffs, 6)

            self.assertTrue(hasattr(diffs[0], "attr"))
            self.assertTrue(hasattr(diffs[0], "local"))
            self.assertTrue(hasattr(diffs[0], "upstream"))

    def test_workflow_type__diff_with_identical_workflow_type(self):
        with patch.object(
            Layer1,
            "describe_workflow_type",
            mock_describe_workflow_type,
        ):
            mocked = mock_describe_workflow_type()
            workflow_type = WorkflowType(
                self.domain,
                name=mocked["typeInfo"]["workflowType"]["name"],
                version=mocked["typeInfo"]["workflowType"]["version"],
                status=mocked["typeInfo"]["status"],
                creation_date=mocked["typeInfo"]["creationDate"],
                deprecation_date=mocked["typeInfo"]["deprecationDate"],
                task_list=mocked["configuration"]["defaultTaskList"]["name"],
                child_policy=mocked["configuration"]["defaultChildPolicy"],
                execution_timeout=mocked["configuration"]["defaultExecutionStartToCloseTimeout"],
                decision_tasks_timeout=mocked["configuration"]["defaultTaskStartToCloseTimeout"],
                description=mocked["typeInfo"]["description"],
            )

            diffs = workflow_type._diff()

            self.assertLength(diffs, 0)

    def test_exists_with_existing_workflow_type(self):
        with patch.object(Layer1, "describe_workflow_type"):
            self.assertTrue(self.wt.exists)

    def test_exists_with_non_existent_workflow_type(self):
        with patch.object(self.wt.connection, "describe_workflow_type") as mock:
            mock.side_effect = SWFResponseError(
                400,
                "Bad Request:",
                {
                    "__type": "com.amazonaws.swf.base.model#UnknownResourceFault",
                    "message": "Unknown type: WorkflowType=[workflowId=blah, runId=test]",
                },
                "UnknownResourceFault",
            )

            self.assertFalse(self.wt.exists)

    # TODO: fix test when no network (probably hits real SWF endpoints)
    @unittest.skip("Skip it in case there's no network connection.")
    def test_workflow_type_exists_with_whatever_error(self):
        with patch.object(self.wt.connection, "describe_workflow_type") as mock:
            with self.assertRaises(ResponseError):
                mock.side_effect = SWFResponseError(
                    400,
                    "mocking exception",
                    {"__type": "WhateverError", "message": "Whatever"},
                )
                _ = self.domain.exists

    def test_is_synced_with_unsynced_workflow_type(self):
        pass

    def test_is_synced_with_synced_workflow_type(self):
        pass

    def test_is_synced_over_non_existent_workflow_type(self):
        with patch.object(Layer1, "describe_workflow_type", mock_describe_workflow_type):
            workflow_type = WorkflowType(
                self.domain,
                "non-existent-workflow-type",
                version="non-existent-version",
            )
            self.assertFalse(workflow_type.is_synced)

    def test_changes_with_different_workflow_type(self):
        with patch.object(
            Layer1,
            "describe_workflow_type",
            mock_describe_workflow_type,
        ):
            workflow_type = WorkflowType(
                self.domain,
                "different-workflow-type-type",
                version="different-workflow-type-type-version",
            )
            diffs = workflow_type.changes

            self.assertIsNotNone(diffs)
            self.assertLength(diffs, 6)

            self.assertTrue(hasattr(diffs[0], "attr"))
            self.assertTrue(hasattr(diffs[0], "local"))
            self.assertTrue(hasattr(diffs[0], "upstream"))

    def test_workflow_type_changes_with_identical_workflow_type(self):
        with patch.object(
            Layer1,
            "describe_workflow_type",
            mock_describe_workflow_type,
        ):
            mocked = mock_describe_workflow_type()
            workflow_type = WorkflowType(
                self.domain,
                name=mocked["typeInfo"]["workflowType"]["name"],
                version=mocked["typeInfo"]["workflowType"]["version"],
                status=mocked["typeInfo"]["status"],
                creation_date=mocked["typeInfo"]["creationDate"],
                deprecation_date=mocked["typeInfo"]["deprecationDate"],
                task_list=mocked["configuration"]["defaultTaskList"]["name"],
                child_policy=mocked["configuration"]["defaultChildPolicy"],
                execution_timeout=mocked["configuration"]["defaultExecutionStartToCloseTimeout"],
                decision_tasks_timeout=mocked["configuration"]["defaultTaskStartToCloseTimeout"],
                description=mocked["typeInfo"]["description"],
            )

            diffs = workflow_type.changes

            self.assertLength(diffs, 0)

    def test_save_already_existing_type(self):
        with patch.object(self.wt.connection, "register_workflow_type") as mock:
            with self.assertRaises(AlreadyExistsError):
                mock.side_effect = SWFTypeAlreadyExistsError(400, "mocked exception")
                self.wt.save()

    def test_save_with_response_error(self):
        with patch.object(self.wt.connection, "register_workflow_type") as mock:
            with self.assertRaises(DoesNotExistError):
                mock.side_effect = SWFResponseError(
                    400,
                    "mocked exception",
                    {"__type": "UnknownResourceFault", "message": "Whatever"},
                )
                self.wt.save()

    def test_delete_non_existent_type(self):
        with patch.object(self.wt.connection, "deprecate_workflow_type") as mock:
            with self.assertRaises(DoesNotExistError):
                mock.side_effect = SWFResponseError(
                    400,
                    "mocked exception",
                    {"__type": "UnknownResourceFault", "message": "Whatever"},
                )
                self.wt.delete()

    def test_delete_deprecated_type(self):
        with patch.object(self.wt.connection, "deprecate_workflow_type") as mock:
            with self.assertRaises(DoesNotExistError):
                mock.side_effect = SWFResponseError(
                    400,
                    "mocked exception",
                    {"__type": "TypeDeprecatedFault", "message": "Whatever"},
                )
                self.wt.delete()


class TestWorkflowExecution(unittest.TestCase, CustomAssertions):
    def setUp(self):
        self.domain = Domain("TestDomain")
        self.wt = WorkflowType(self.domain, "TestType", "1.0")
        self.we = WorkflowExecution(self.domain, self.wt, "TestType-0.1-TestDomain")

    def tearDown(self):
        pass

    def test_instantiation(self):
        we = WorkflowExecution(self.domain, self.wt, "TestType-0.1-TestDomain")
        self.assertIsNotNone(we)
        self.assertIsInstance(we, WorkflowExecution)
        self.assertIn(we.status, [WorkflowExecution.STATUS_OPEN, WorkflowExecution.STATUS_CLOSED])

    def test___diff_with_different_workflow_execution(self):
        with patch.object(
            Layer1,
            "describe_workflow_execution",
            mock_describe_workflow_execution,
        ):
            workflow_execution = WorkflowExecution(
                self.domain,
                WorkflowType(self.domain, "NonExistentTestType", "1.0"),
                "non-existent-id",
            )
            diffs = workflow_execution._diff()

            self.assertIsNotNone(diffs)
            self.assertLength(diffs, 7)

            self.assertTrue(hasattr(diffs[0], "attr"))
            self.assertTrue(hasattr(diffs[0], "local"))
            self.assertTrue(hasattr(diffs[0], "upstream"))

    def test_workflow_execution__diff_with_identical_workflow_execution(self):
        with patch.object(
            Layer1,
            "describe_workflow_execution",
            mock_describe_workflow_execution,
        ):
            mocked = mock_describe_workflow_execution()
            workflow_execution = WorkflowExecution(
                self.domain,
                mocked["executionInfo"]["execution"]["workflowId"],
                run_id=mocked["executionInfo"]["execution"]["runId"],
                status=mocked["executionInfo"]["executionStatus"],
                task_list=mocked["executionConfiguration"]["taskList"]["name"],
                child_policy=mocked["executionConfiguration"]["childPolicy"],
                execution_timeout=mocked["executionConfiguration"]["executionStartToCloseTimeout"],
                tag_list=mocked["executionInfo"]["tagList"],
                decision_tasks_timeout=mocked["executionConfiguration"]["taskStartToCloseTimeout"],
            )

            diffs = workflow_execution._diff()

            self.assertLength(diffs, 0)

    def test_exists_with_existing_workflow_execution(self):
        with patch.object(Layer1, "describe_workflow_execution"):
            self.assertTrue(self.we.exists)

    def test_exists_with_non_existent_workflow_execution(self):
        with patch.object(self.we.connection, "describe_workflow_execution") as mock:
            mock.side_effect = SWFResponseError(
                400,
                "Bad Request:",
                {
                    "__type": "com.amazonaws.swf.base.model#UnknownResourceFault",
                    "message": "Unknown execution: WorkflowExecution=[workflowId=blah, runId=test]",
                },
                "UnknownResourceFault",
            )

            self.assertFalse(self.we.exists)

    # TODO: fix test when no network (probably hits real SWF endpoints)
    @unittest.skip("Skip it in case there's no network connection.")
    def test_workflow_execution_exists_with_whatever_error(self):
        with patch.object(self.we.connection, "describe_workflow_execution") as mock:
            with self.assertRaises(ResponseError):
                mock.side_effect = SWFResponseError(
                    400,
                    "mocking exception",
                    {"__type": "WhateverError", "message": "Whatever"},
                )
                _ = self.domain.exists

    def test_is_synced_with_unsynced_workflow_execution(self):
        pass

    def test_is_synced_with_synced_workflow_execution(self):
        pass

    def test_is_synced_over_non_existent_workflow_execution(self):
        with patch.object(Layer1, "describe_workflow_execution", mock_describe_workflow_execution):
            workflow_execution = WorkflowExecution(
                self.domain,
                WorkflowType(self.domain, "NonExistentTestType", "1.0"),
                "non-existent-id",
            )
            self.assertFalse(workflow_execution.is_synced)

    def test_changes_with_different_workflow_execution(self):
        with patch.object(
            Layer1,
            "describe_workflow_execution",
            mock_describe_workflow_execution,
        ):
            workflow_execution = WorkflowExecution(
                self.domain,
                WorkflowType(self.domain, "NonExistentTestType", "1.0"),
                "non-existent-id",
            )
            diffs = workflow_execution.changes

            self.assertIsNotNone(diffs)
            self.assertLength(diffs, 7)

            self.assertTrue(hasattr(diffs[0], "attr"))
            self.assertTrue(hasattr(diffs[0], "local"))
            self.assertTrue(hasattr(diffs[0], "upstream"))

    def test_workflow_execution_changes_with_identical_workflow_execution(self):
        with patch.object(
            Layer1,
            "describe_workflow_execution",
            mock_describe_workflow_execution,
        ):
            mocked = mock_describe_workflow_execution()
            workflow_execution = WorkflowExecution(
                self.domain,
                mocked["executionInfo"]["execution"]["workflowId"],
                run_id=mocked["executionInfo"]["execution"]["runId"],
                status=mocked["executionInfo"]["executionStatus"],
                task_list=mocked["executionConfiguration"]["taskList"]["name"],
                child_policy=mocked["executionConfiguration"]["childPolicy"],
                execution_timeout=mocked["executionConfiguration"]["executionStartToCloseTimeout"],
                tag_list=mocked["executionInfo"]["tagList"],
                decision_tasks_timeout=mocked["executionConfiguration"]["taskStartToCloseTimeout"],
            )

            diffs = workflow_execution.changes

            self.assertLength(diffs, 0)

    def test_history(self):
        with patch.object(
            self.we.connection,
            "get_workflow_execution_history",
            mock_get_workflow_execution_history,
        ):
            history = self.we.history()
            self.assertIsInstance(history, History)
