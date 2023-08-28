from __future__ import annotations

import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

import simpleflow.swf.mapper.settings
from simpleflow.swf.mapper.core import ConnectedSWFObject
from simpleflow.swf.mapper.exceptions import DoesNotExistError, ResponseError
from simpleflow.swf.mapper.models.domain import Domain
from simpleflow.swf.mapper.querysets.domain import DomainQuerySet

from ..mocks.domain import mock_describe_domain, mock_list_domains

simpleflow.swf.mapper.settings.set(aws_access_key_id="fakeaccesskey", aws_secret_access_key="fakesecret")


class TestDomainQuerySet(unittest.TestCase):
    def setUp(self):
        self.domain = Domain("test-domain")
        self.qs = DomainQuerySet()

    def tearDown(self):
        pass

    def test_get_existent_domain(self):
        with patch.object(ConnectedSWFObject, "describe_domain", mock_describe_domain):
            domain = self.qs.get("test-domain")
            self.assertIsInstance(domain, Domain)

            self.assertTrue(hasattr(domain, "name"))
            self.assertEqual(domain.name, "test-domain")

            self.assertTrue(hasattr(domain, "status"))
            self.assertEqual(domain.status, self.domain.status)

    def test_get_raising_domain(self):
        with patch.object(ConnectedSWFObject, "describe_domain") as mock:
            with self.assertRaises(ResponseError):
                mock.side_effect = ClientError(
                    {
                        "Error": {
                            "Message": "Foo bar",
                            "Code": "WhateverError",
                        },
                        "message": "Foo bar",
                    },
                    "describe_domain",
                )
                self.qs.get("whatever")

    def test_get_or_create_existing_domain(self):
        with patch.object(ConnectedSWFObject, "describe_domain", mock_describe_domain):
            domain = self.qs.get_or_create("TestDomain")

            self.assertIsInstance(domain, Domain)

    def test_get_or_create_non_existent_domain(self):
        with patch.object(ConnectedSWFObject, "describe_domain") as mock:
            mock.side_effect = DoesNotExistError("Mocked exception")

            with patch.object(ConnectedSWFObject, "register_domain", mock_describe_domain):
                domain = self.qs.get_or_create("TestDomain")

                self.assertIsInstance(domain, Domain)

    def test_all_with_existent_domains(self):
        with patch.object(self.qs, "list_domains", mock_list_domains):
            domains = self.qs.all()
            self.assertEqual(len(domains), 1)
            self.assertIsInstance(domains[0], Domain)

    def test_create_domain(self):
        with patch.object(ConnectedSWFObject, "register_domain"):
            new_domain = self.qs.create("TestDomain")

            self.assertIsNotNone(new_domain)
            self.assertIsInstance(new_domain, Domain)
