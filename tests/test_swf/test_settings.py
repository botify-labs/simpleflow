from __future__ import annotations

import os
import unittest

from swf.core import ConnectedSWFObject
from swf.settings import clear, from_env

AWS_ENV_KEYS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
)


class TestSettings(unittest.TestCase):
    def setUp(self):
        self.oldies = {}
        for key in AWS_ENV_KEYS:
            self.oldies[key] = os.environ.get(key)
            os.environ.pop(key, None)

    def tearDown(self):
        for key in AWS_ENV_KEYS:
            if self.oldies[key]:
                os.environ[key] = self.oldies[key]
            else:
                os.environ.pop(key, None)

    def test_get_aws_settings_with_region(self):
        """
        AWS_DEFAULT_REGION is parsed correctly for settings.
        """
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"
        _settings = from_env()
        self.assertEqual(_settings, {"region": "eu-west-1"})

    def test_get_aws_settings_with_access_key_id(self):
        """
        Even if AWS_ACCESS_KEY_ID is set in env, don't pass it to settings.
        """
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "bar"
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"
        _settings = from_env()
        self.assertEqual(_settings, {"region": "eu-west-1"})

    def test_get_aws_settings_without_access_key_id(self):
        """
        If AWS_DEFAULT_REGION is not set, get AWS_DEFAULT_REGION from env anyway if there.
        """
        self.assertEqual(from_env(), {})

        os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"
        self.assertEqual(
            from_env(),
            {
                "region": "eu-west-1",
            },
        )

    def test_get_aws_connection_with_key(self):
        """
        If AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and/or AWS_SECURITY_TOKEN
        are set in environment, they are present in the boto connection.

        """
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "bar"
        os.environ["AWS_SECURITY_TOKEN"] = "baz"
        # Clear any global settings from other tests.
        clear()
        obj = ConnectedSWFObject()
        self.assertEqual(obj.connection.aws_access_key_id, "foo")
        self.assertEqual(obj.connection.aws_secret_access_key, "bar")
        self.assertEqual(obj.connection.provider.security_token, "baz")
