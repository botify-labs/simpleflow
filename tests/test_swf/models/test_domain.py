from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from boto.exception import SWFResponseError
from boto.swf.exceptions import SWFDomainAlreadyExistsError
from boto.swf.layer1 import Layer1

import swf.settings
from swf.constants import DEPRECATED
from swf.exceptions import AlreadyExistsError, ResponseError
from swf.models.domain import Domain, DomainDoesNotExist
from swf.querysets.domain import DomainQuerySet
from swf.querysets.workflow import WorkflowTypeQuerySet
from tests.test_swf.mocks.base import MiniMock
from tests.test_swf.mocks.domain import mock_describe_domain

swf.settings.set(aws_access_key_id="fakeaccesskey", aws_secret_access_key="fakesecret")


class TestDomain(unittest.TestCase):
    def setUp(self):
        self.domain = Domain("testdomain")
        self.qs = DomainQuerySet(self)

        self.mocked_workflow_type_qs = Mock(spec=WorkflowTypeQuerySet)
        self.mocked_workflow_type_qs.all.return_value = []

    def tearDown(self):
        pass

    @patch.object(
        Layer1,
        "__init__",
        MiniMock(aws_access_key_id="test", aws_secret_access_key="test"),
    )
    def test_domain_inits_connection(self):
        self.assertTrue(hasattr(self.domain, "connection"))
        self.assertTrue(hasattr(self.domain.connection, "aws_access_key_id"))
        self.assertTrue(hasattr(self.domain.connection, "aws_secret_access_key"))

    def test_domain__diff_with_different_domain(self):
        with patch.object(
            Layer1,
            "describe_domain",
            mock_describe_domain,
        ):
            domain = Domain("different-domain", status=DEPRECATED, description="blabla")
            diffs = domain._diff()

            self.assertIsNotNone(diffs)
            self.assertEqual(len(diffs), 4)

            self.assertTrue(hasattr(diffs[0], "attr"))
            self.assertTrue(hasattr(diffs[0], "local"))
            self.assertTrue(hasattr(diffs[0], "upstream"))

    def test_domain__diff_with_identical_domain(self):
        with patch.object(
            Layer1,
            "describe_domain",
            mock_describe_domain,
        ):
            mocked = mock_describe_domain()
            domain = Domain(
                mocked["domainInfo"]["name"],
                status=mocked["domainInfo"]["status"],
                description=mocked["domainInfo"]["description"],
                retention_period=mocked["configuration"]["workflowExecutionRetentionPeriodInDays"],
            )

            diffs = domain._diff()

            self.assertEqual(len(diffs), 0)

    def test_domain_exists_with_existing_domain(self):
        with patch.object(self.domain.connection, "describe_domain"):
            self.assertTrue(self.domain.exists)

    def test_domain_exists_with_non_existent_domain(self):
        with patch.object(self.domain.connection, "describe_domain") as mock:
            mock.side_effect = SWFResponseError(
                400,
                "Bad Request",
                {
                    "message": "Unknown domain: does not exist",
                    "__type": "com.amazonaws.swf.base.model#UnknownResourceFault",
                },
                "UnknownResourceFault",
            )

            self.assertFalse(self.domain.exists)

    def test_domain_exists_with_whatever_error(self):
        with patch.object(self.domain.connection, "describe_domain") as mock:
            with self.assertRaises(ResponseError):
                mock.side_effect = SWFResponseError(
                    400,
                    "mocking exception",
                    {"__type": "WhateverError", "message": "Whatever"},
                )
                _ = self.domain.exists

    def test_domain_is_synced_with_unsynced_domain(self):
        pass

    def test_domain_is_synced_with_synced_domain(self):
        pass

    def test_domain_is_synced_over_non_existent_domain(self):
        with patch.object(Layer1, "describe_domain", mock_describe_domain):
            domain = Domain("non-existent-domain")
            self.assertFalse(domain.is_synced)

    def test_domain_changes_with_different_domain(self):
        with patch.object(
            Layer1,
            "describe_domain",
            mock_describe_domain,
        ):
            domain = Domain("different-domain", status=DEPRECATED, description="blabla")
            diffs = domain.changes

            self.assertIsNotNone(diffs)
            self.assertEqual(len(diffs), 4)

            self.assertTrue(hasattr(diffs[0], "attr"))
            self.assertTrue(hasattr(diffs[0], "local"))
            self.assertTrue(hasattr(diffs[0], "upstream"))

    def test_domain_changes_with_identical_domain(self):
        with patch.object(
            Layer1,
            "describe_domain",
            mock_describe_domain,
        ):
            mocked = mock_describe_domain()
            domain = Domain(
                mocked["domainInfo"]["name"],
                status=mocked["domainInfo"]["status"],
                description=mocked["domainInfo"]["description"],
                retention_period=mocked["configuration"]["workflowExecutionRetentionPeriodInDays"],
            )

            diffs = domain.changes

            self.assertEqual(len(diffs), 0)

    def test_domain_save_valid_domain(self):
        with patch.object(self.domain.connection, "register_domain"):
            self.domain.save()

    def test_domain_save_already_existing_domain(self):
        with patch.object(self.domain.connection, "register_domain") as mock:
            with self.assertRaises(AlreadyExistsError):
                mock.side_effect = SWFDomainAlreadyExistsError(400, "mocking exception")
                self.domain.save()

    def test_domain_delete_existing_domain(self):
        with patch.object(self.domain.connection, "deprecate_domain"):
            self.domain.delete()

    def test_domain_delete_non_existent_domain(self):
        with patch.object(self.domain.connection, "deprecate_domain") as mock:
            with self.assertRaises(DomainDoesNotExist):
                mock.side_effect = SWFResponseError(
                    400,
                    "Bad Request",
                    {
                        "message": "Unknown domain: does not exist",
                        "__type": "com.amazonaws.swf.base.model#UnknownResourceFault",
                    },
                    "UnknownResourceFault",
                )
                self.domain.delete()

    def test_domain_workflows_without_existent_workflows(self):
        with patch.object(WorkflowTypeQuerySet, "all") as all_method:
            all_method.return_value = []
            self.assertEqual(self.domain.workflows(), [])
