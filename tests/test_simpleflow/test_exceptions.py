from __future__ import annotations

import unittest
from unittest.mock import patch

import boto
from sure import expect

from simpleflow.exceptions import TaskFailed
from simpleflow.storage import push_content
from tests.moto_compat import mock_s3


class TestTaskFailed(unittest.TestCase):
    def test_task_failed_representation(self):
        failure = TaskFailed("message", None, None)
        expect(str(failure)).to.equal("('message', None, None)")
        expect(repr(failure)).to.equal('TaskFailed (message, "None")')

        failure = TaskFailed("message", "reason", "detail")
        expect(str(failure)).to.equal("('message', 'reason', 'detail')")
        expect(repr(failure)).to.equal('TaskFailed (message, "reason")')

    @mock_s3
    @patch.dict("os.environ", {"SIMPLEFLOW_JUMBO_FIELDS_BUCKET": "jumbo-bucket"})
    def test_task_failed_jumbo_fields_resolution(self):
        # prepare jumbo field content
        boto.connect_s3().create_bucket("jumbo-bucket")
        push_content("jumbo-bucket", "my-reason", "reason decoded!")
        push_content("jumbo-bucket", "my-details", "details decoded!")

        # test resolution
        failure = TaskFailed(
            "message",
            "simpleflow+s3://jumbo-bucket/my-reason 17",
            "simpleflow+s3://jumbo-bucket/my-details 17",
        )
        # TODO: maybe override __str__() ourselves to get rid of those ugly u'' in python 2.x
        expect(str(failure)).to.match(r"^\('message', u?'reason decoded!', u?'details decoded!'\)$")
        expect(repr(failure)).to.equal('TaskFailed (message, "reason decoded!")')
