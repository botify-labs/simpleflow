import os
import unittest

from swf.settings import from_env

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

    def test_get_aws_settings_with_access_key_id(self):
        """
        If AWS_ACCESS_KEY_ID is set, get all 3 params from env.
        """
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "bar"
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"
        _settings = from_env()
        self.assertEqual(_settings, {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "region": "eu-west-1",
        })

    def test_get_aws_settings_without_access_key_id(self):
        """
        If AWS_DEFAULT_REGION is not set, get AWS_DEFAULT_REGION from env anyway if there.
        """
        self.assertEqual(from_env(), {})

        os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"
        self.assertEqual(from_env(), {
            "region": "eu-west-1",
        })
