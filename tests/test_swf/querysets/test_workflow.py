from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from boto.exception import SWFResponseError
from boto.swf.layer1 import Layer1

from swf.constants import REGISTERED
from swf.exceptions import DoesNotExistError, ResponseError
from swf.models.domain import Domain
from swf.models.workflow import WorkflowExecution, WorkflowType
from swf.querysets.workflow import (
    BaseWorkflowQuerySet,
    WorkflowExecutionQuerySet,
    WorkflowTypeQuerySet,
)
from swf.utils import datetime_timestamp, past_day

from ..mocks.workflow import (
    mock_describe_workflow_type,
    mock_list_closed_workflow_executions,
    mock_list_open_workflow_executions,
    mock_list_workflow_types,
)


class TestBaseWorkflowTypeQuerySet(unittest.TestCase):
    def setUp(self):
        self.domain = Domain("TestDomain")
        self.bw = BaseWorkflowQuerySet(self.domain)

    def tearDown(self):
        pass

    def test_get_domain_property_instantiates_private_attribute(self):
        bw = BaseWorkflowQuerySet(self.domain)
        delattr(bw, "_domain")
        _ = bw.domain

        self.assertTrue(hasattr(bw, "_domain"))

    def test_instantiation_with_valid_domain(self):
        bw = BaseWorkflowQuerySet(self.domain)

        self.assertIsInstance(bw.domain, Domain)
        self.assertEqual(bw._domain, bw.domain)

    def test_instantiation_with_invalid_domain(self):
        with self.assertRaises(TypeError):
            BaseWorkflowQuerySet("WrongType")

    def test__list_isnt_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.bw._list()


class TestWorkflowTypeQuerySet(unittest.TestCase):
    def setUp(self):
        self.domain = Domain("TestDomain")
        self.wtq = WorkflowTypeQuerySet(self.domain)

    def tearDown(self):
        pass

    def test_valid_workflow_type(self):
        with patch.object(self.wtq.connection, "describe_workflow_type", mock_describe_workflow_type):
            wt = self.wtq.get("TestType", "0.1")
            self.assertIsNotNone(wt)
            self.assertIsInstance(wt, WorkflowType)

    def test_get_non_existent_workflow_type(self):
        with patch.object(self.wtq.connection, "describe_workflow_type") as mock:
            with self.assertRaises(DoesNotExistError):
                mock.side_effect = SWFResponseError(
                    400,
                    "mocked exception",
                    {
                        "__type": "UnknownResourceFault",
                        "message": "Whatever",
                    },
                )
                self.wtq.get("NonExistentWorkflowType", "0.1")

    def test_get_whatever_failing_workflow_type(self):
        with patch.object(self.wtq.connection, "describe_workflow_type") as mock:
            with self.assertRaises(ResponseError):
                mock.side_effect = SWFResponseError(
                    400,
                    "mocked exception",
                    {
                        "__type": "Whatever Error",
                        "message": "Whatever",
                    },
                )
                self.wtq.get("NonExistentWorkflowType", "0.1")

    def test_get_or_create_existing_workflow_type(self):
        with patch.object(Layer1, "describe_workflow_type", mock_describe_workflow_type):
            workflow_type = self.wtq.get_or_create("TestActivityType", "testversion")

            self.assertIsInstance(workflow_type, WorkflowType)

    def test_get_or_create_non_existent_workflow_type(self):
        with patch.object(Layer1, "describe_workflow_type") as mock:
            mock.side_effect = DoesNotExistError("Mocked exception")

            with patch.object(Layer1, "register_workflow_type", mock_describe_workflow_type):
                workflow_type = self.wtq.get_or_create("TestDomain", "testversion")

                self.assertIsInstance(workflow_type, WorkflowType)

    def test__list_non_empty_workflow_types(self):
        with patch.object(self.wtq.connection, "list_workflow_types", mock_list_workflow_types):
            wt = self.wtq._list()
            self.assertIsNotNone(wt)
            self.assertIsInstance(wt, dict)

            for workflow in wt[WorkflowTypeQuerySet._infos_plural]:
                self.assertIsInstance(workflow, dict)

    def test_filter_with_registered_status(self):
        # Nota: mock_list_workfflow_types returned
        # values are REGISTERED
        with patch.object(self.wtq.connection, "list_workflow_types", mock_list_workflow_types):
            types = self.wtq.filter(registration_status=REGISTERED)
            self.assertIsNotNone(types)
            self.assertIsInstance(types, list)

            for wt in types:
                self.assertIsInstance(wt, WorkflowType)
                self.assertEqual(wt.status, REGISTERED)

    def test_create_workflow_type(self):
        with patch.object(Layer1, "register_workflow_type"):
            new_wt = self.wtq.create(
                self.domain,
                "TestWorkflowType",
                "0.test",
            )

            self.assertIsNotNone(new_wt)
            self.assertIsInstance(new_wt, WorkflowType)


class TestWorkflowExecutionQuerySet(unittest.TestCase):
    def setUp(self):
        self.domain = Domain("TestDomain")
        self.wt = WorkflowType(self.domain, "TestType", "0.1")
        self.weq = WorkflowExecutionQuerySet(self.domain)

    def tearDown(self):
        pass

    def test__is_valid_open_status_param(self):
        status = WorkflowExecution.STATUS_OPEN

        self.assertTrue(
            self.weq._is_valid_status_param(status, "oldest_date"),
        )

    def test__is_invalid_open_status_param(self):
        status = WorkflowExecution.STATUS_OPEN

        self.assertFalse(
            self.weq._is_valid_status_param(status, "start_oldest_date"),
        )

    def test__is_valid_closed_status_param(self):
        status = WorkflowExecution.STATUS_CLOSED

        self.assertTrue(
            self.weq._is_valid_status_param(status, "start_oldest_date"),
        )

    def test__is_invalid_closed_status_param(self):
        status = WorkflowExecution.STATUS_CLOSED

        self.assertFalse(
            self.weq._is_valid_status_param(status, "oldest_date"),
        )

    def test_validate_valid_open_status_parameters(self):
        params = ["oldest_date", "latest_date"]
        status = WorkflowExecution.STATUS_OPEN

        self.assertEqual(self.weq._validate_status_parameters(status, params), [])

    def test_validate_invalid_open_status_parameters(self):
        params = ["oldest_date", "start_latest_date"]
        status = WorkflowExecution.STATUS_OPEN

        self.assertEqual(self.weq._validate_status_parameters(status, params), ["start_latest_date"])

    def test_validate_valid_closed_status_parameters(self):
        params = ["start_oldest_date", "start_latest_date"]
        status = WorkflowExecution.STATUS_CLOSED

        self.assertEqual(self.weq._validate_status_parameters(status, params), [])

    def test_validate_invalid_closed_status_parameters(self):
        params = ["oldest_date", "start_latest_date"]
        status = WorkflowExecution.STATUS_CLOSED

        self.assertEqual(self.weq._validate_status_parameters(status, params), ["oldest_date"])

    def test_list_open_workflows_executions_with_start_oldest_date(self):
        with patch.object(
            self.weq.connection,
            "list_open_workflow_executions",
            mock_list_open_workflow_executions,
        ):
            we = self.weq.list_workflow_executions(
                WorkflowExecution.STATUS_OPEN,
                self.domain.name,
                start_oldest_date=int(datetime_timestamp(past_day(3))),
            )
            self.assertIsNotNone(we)
            self.assertIsInstance(we, dict)

            self.assertTrue(we["executionInfos"][0]["executionStatus"] == WorkflowExecution.STATUS_OPEN)

    def test_list_closed_workflows_executions(self):
        with patch.object(
            self.weq.connection,
            "list_closed_workflow_executions",
            mock_list_closed_workflow_executions,
        ):
            we = self.weq.list_workflow_executions(
                WorkflowExecution.STATUS_CLOSED,
                self.domain.name,
                start_oldest_date=int(datetime_timestamp(past_day(3))),
            )
            self.assertIsNotNone(we)
            self.assertIsInstance(we, dict)

            self.assertTrue(we["executionInfos"][0]["executionStatus"] == WorkflowExecution.STATUS_CLOSED)

    def test_list_invalid_status_workflow_executions(self):
        with self.assertRaises(ValueError):
            self.weq.list_workflow_executions(
                "INVALID_STATUS",
                self.domain.name,
                start_oldest_date=int(datetime_timestamp(past_day(3))),
            )

    def test_get_workflow_type(self):
        execution_info = mock_list_open_workflow_executions()["executionInfos"][0]

        with patch.object(Layer1, "describe_workflow_type", mock_describe_workflow_type):
            wt = self.weq.get_workflow_type(execution_info)
            self.assertIsNotNone(wt)
            self.assertIsInstance(wt, WorkflowType)
            self.assertTrue(wt.name == execution_info["workflowType"]["name"])

    def test_get_valid_workflow_execution(self):
        pass
        # with patch.object(
        #     self.weq.connection,
        #     'describe_workflow_execution',
        #     mock_describe_workflow_execution
        # ):
        #     we = self.weq.get("mocked-workflow-id", "mocked-run-id")
        #     self.assertIsNotNone(we)
        #     self.assertIsInstance(we, WorkflowExecution)

    def test_get_non_existent_workflow_execution(self):
        with patch.object(self.weq.connection, "describe_workflow_execution") as mock:
            with self.assertRaises(DoesNotExistError):
                mock.side_effect = SWFResponseError(
                    400,
                    "mocked exception",
                    {
                        "__type": "UnknownResourceFault",
                        "message": "Whatever",
                    },
                )
                self.weq.get("mocked-workflow-id", "mocked-run-id")

    def test_get_invalid_workflow_execution(self):
        with patch.object(self.weq.connection, "describe_workflow_execution") as mock:
            with self.assertRaises(ResponseError):
                mock.side_effect = SWFResponseError(
                    400,
                    "mocked exception",
                    {
                        "__type": "WhateverFault",
                        "message": "Whatever",
                    },
                )
                self.weq.get("mocked-workflow-id", "mocked-run-id")

    def test_filter_without_close_time_filter(self):
        self.weq._list_items = Mock(return_value=[])
        _ = self.weq.filter()
        self.weq._list_items.assert_called_once()
        kwargs = self.weq._list_items.call_args[1]
        self.assertIsInstance(kwargs["start_oldest_date"], int)

    def test_filter_with_close_time_filter(self):
        self.weq._list_items = Mock(return_value=[])
        _ = self.weq.filter(status=WorkflowExecution.STATUS_CLOSED, close_latest_date=5)
        self.weq._list_items.assert_called_once()
        kwargs = self.weq._list_items.call_args[1]
        self.assertIsNone(kwargs["start_oldest_date"])
        self.assertIsInstance(kwargs["close_latest_date"], int)
