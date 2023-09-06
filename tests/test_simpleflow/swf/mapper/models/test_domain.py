from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import boto3
from botocore.exceptions import ClientError
from moto import mock_swf

import simpleflow.swf.mapper.settings
from simpleflow.swf.mapper.constants import DEPRECATED
from simpleflow.swf.mapper.core import ConnectedSWFObject
from simpleflow.swf.mapper.exceptions import AlreadyExistsError
from simpleflow.swf.mapper.models.domain import Domain, DomainDoesNotExist
from simpleflow.swf.mapper.querysets.domain import DomainQuerySet
from simpleflow.swf.mapper.querysets.workflow import WorkflowTypeQuerySet
from tests.test_simpleflow.swf.mapper.mocks.domain import mock_describe_domain

simpleflow.swf.mapper.settings.set(aws_access_key_id="fakeaccesskey", aws_secret_access_key="fakesecret")


class TestDomain(unittest.TestCase):
    def setUp(self):
        self.domain = Domain("testdomain")
        self.qs = DomainQuerySet(self)

        self.mocked_workflow_type_qs = Mock(spec=WorkflowTypeQuerySet)
        self.mocked_workflow_type_qs.all.return_value = []

    def tearDown(self):
        pass

    def test_domain__diff_with_different_domain(self):
        with patch.object(
            ConnectedSWFObject,
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
            ConnectedSWFObject,
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
        with patch.object(ConnectedSWFObject, "describe_domain"):
            self.assertTrue(self.domain.exists)

    @mock_swf
    def test_domain_exists_with_non_existent_domain(self):
        self.assertFalse(self.domain.exists)

    # TODO: test this maybe, but moto doesn't support permission errors and we don't want to mock the implementation
    # def test_domain_exists_with_whatever_error(self):
    #     pass

    def test_domain_is_synced_with_unsynced_domain(self):
        pass

    def test_domain_is_synced_with_synced_domain(self):
        pass

    def test_domain_is_synced_over_non_existent_domain(self):
        with patch.object(ConnectedSWFObject, "describe_domain", mock_describe_domain):
            domain = Domain("non-existent-domain")
            self.assertFalse(domain.is_synced)

    def test_domain_changes_with_different_domain(self):
        with patch.object(
            ConnectedSWFObject,
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

    def test_domain_save_valid_domain(self):
        with patch.object(self.domain, "register_domain"):
            self.domain.save()

    def test_domain_save_already_existing_domain(self):
        with patch.object(self.domain, "register_domain") as mock:
            with self.assertRaises(AlreadyExistsError):
                mock.side_effect = ClientError(
                    {
                        "Error": {"Message": "testdomain", "Code": "DomainAlreadyExistsFault"},
                        "message": "testdomain",
                    },
                    "register_domain",
                )
                self.domain.save()

    @mock_swf
    def test_domain_delete_existing_domain(self):
        client = boto3.client("swf", region_name="us-east-1")
        client.register_domain(
            name="test-domain",
            workflowExecutionRetentionPeriodInDays="10",
        )

        def list_domains():
            return client.list_domains(
                registrationStatus="REGISTERED",
            )["domainInfos"]

        # existent domain
        assert len(list_domains()) == 1
        Domain("test-domain").delete()
        assert len(list_domains()) == 0

        with self.assertRaises(DomainDoesNotExist):
            Domain("non-existent-domain").delete()

    def test_domain_workflows_without_existent_workflows(self):
        with patch.object(WorkflowTypeQuerySet, "all") as all_method:
            all_method.return_value = []
            self.assertEqual(self.domain.workflows(), [])
