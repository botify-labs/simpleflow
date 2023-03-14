from __future__ import annotations

import unittest
from unittest.mock import patch

from boto.exception import SWFResponseError
from boto.swf.layer1 import Layer1

import swf.settings
from swf.exceptions import DoesNotExistError, InvalidCredentialsError, ResponseError
from swf.models.domain import Domain
from swf.querysets.domain import DomainQuerySet

from ..mocks.domain import mock_describe_domain, mock_list_domains

swf.settings.set(aws_access_key_id="fakeaccesskey", aws_secret_access_key="fakesecret")


class TestDomainQuerySet(unittest.TestCase):
    def setUp(self):
        self.domain = Domain("test-domain")
        self.qs = DomainQuerySet()

    def tearDown(self):
        pass

    def test_get_existent_domain(self):
        with patch.object(self.qs.connection, "describe_domain", mock_describe_domain):
            domain = self.qs.get("test-domain")
            self.assertIsInstance(domain, Domain)

            self.assertTrue(hasattr(domain, "name"))
            self.assertEqual(domain.name, "test-domain")

            self.assertTrue(hasattr(domain, "status"))
            self.assertEqual(domain.status, self.domain.status)

    def test_get_non_existent_domain(self):
        with patch.object(self.qs.connection, "describe_domain") as mock:
            with self.assertRaises(DoesNotExistError):
                mock.side_effect = SWFResponseError(400, "mocking exception", {"__type": "UnknownResourceFault"})
                self.qs.get("non_existent")

    def test_get_domain_with_invalid_credentials(self):
        with patch.object(self.qs.connection, "describe_domain") as mock:
            with self.assertRaises(InvalidCredentialsError):
                mock.side_effect = SWFResponseError(400, "mocking exception", {"__type": "UnrecognizedClientException"})
                self.qs.get("non_existent")

    def test_get_raising_domain(self):
        with patch.object(self.qs.connection, "describe_domain") as mock:
            with self.assertRaises(ResponseError):
                mock.side_effect = SWFResponseError(
                    400,
                    "mocking exception",
                    {
                        "__type": "WhateverError",
                        "message": "WhateverMessage",
                    },
                )
                self.qs.get("whatever")

    def test_get_or_create_existing_domain(self):
        with patch.object(Layer1, "describe_domain", mock_describe_domain):
            domain = self.qs.get_or_create("TestDomain")

            self.assertIsInstance(domain, Domain)

    def test_get_or_create_non_existent_domain(self):
        with patch.object(Layer1, "describe_domain") as mock:
            mock.side_effect = DoesNotExistError("Mocked exception")

            with patch.object(Layer1, "register_domain", mock_describe_domain):
                domain = self.qs.get_or_create("TestDomain")

                self.assertIsInstance(domain, Domain)

    def test_all_with_existent_domains(self):
        with patch.object(self.qs.connection, "list_domains", mock_list_domains):
            domains = self.qs.all()
            self.assertEqual(len(domains), 1)
            self.assertIsInstance(domains[0], Domain)

    def test_create_domain(self):
        with patch.object(Layer1, "register_domain"):
            new_domain = self.qs.create("TestDomain")

            self.assertIsNotNone(new_domain)
            self.assertIsInstance(new_domain, Domain)
