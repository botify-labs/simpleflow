from __future__ import annotations

import re
import unittest
from unittest.mock import patch

import boto3
from moto import mock_s3

from simpleflow.exceptions import TaskFailed
from simpleflow.storage import push_content


class TestTaskFailed(unittest.TestCase):
    def test_task_failed_representation(self):
        failure = TaskFailed("message", None, None)
        assert str(failure) == "('message', None, None)"
        assert repr(failure) == 'TaskFailed (message, "None")'

        failure = TaskFailed("message", "reason", "detail")
        assert str(failure) == "('message', 'reason', 'detail')"
        assert repr(failure) == 'TaskFailed (message, "reason")'

    @mock_s3
    @patch.dict("os.environ", {"SIMPLEFLOW_JUMBO_FIELDS_BUCKET": "jumbo-bucket"})
    def test_task_failed_jumbo_fields_resolution(self):
        # prepare jumbo field content
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="jumbo-bucket")
        push_content("jumbo-bucket", "my-reason", "reason decoded!")
        push_content("jumbo-bucket", "my-details", "details decoded!")

        # test resolution
        failure = TaskFailed(
            "message",
            "simpleflow+s3://jumbo-bucket/my-reason 17",
            "simpleflow+s3://jumbo-bucket/my-details 17",
        )
        # TODO: maybe override __str__() ourselves to get rid of those ugly u'' in python 2.x
        assert re.search(r"^\('message', u?'reason decoded!', u?'details decoded!'\)$", str(failure))
        assert repr(failure) == 'TaskFailed (message, "reason decoded!")'
