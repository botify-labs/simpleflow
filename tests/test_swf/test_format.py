import json
import os
import unittest
import random

import boto

try:
    from moto import mock_s3_deprecated as mock_s3
except ImportError:
    from moto import mock_s3

import swf.format
import swf.constants


@mock_s3
class TestFormat(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        # reset jumbo fields bucket so most tests will have jumbo fields disabled
        os.environ["SIMPLEFLOW_JUMBO_FIELDS_BUCKET"] = ""

    def setup_jumbo_fields(self, bucket):
        os.environ["SIMPLEFLOW_JUMBO_FIELDS_BUCKET"] = bucket
        self.conn = boto.connect_s3()
        self.conn.create_bucket(bucket.split("/")[0])

    def test_encode_none(self):
        self.assertEquals(
            swf.format.encode(None, 1),
            None
        )

    def test_encode_smaller(self):
        MAX_LENGTH = random.randint(10, 1000)
        message = 'A' * (MAX_LENGTH // 2)
        self.assertEquals(
            swf.format.encode(message, MAX_LENGTH),
            message,
        )

    def test_encode_longer(self):
        MAX_LENGTH = random.randint(10, 1000)
        message = 'A' * 1000
        with self.assertRaisesRegexp(ValueError, "Message too long"):
            swf.format.encode(message, MAX_LENGTH)

    def test_identity_doesnt_use_jumbo_fields(self):
        self.setup_jumbo_fields("jumbo-bucket")
        message = 'A' * (swf.constants.MAX_RESULT_LENGTH * 2)
        with self.assertRaisesRegexp(ValueError, "Message too long"):
            swf.format.identity(message)

    def test_jumbo_fields_encoding_without_directory(self):
        self.setup_jumbo_fields("jumbo-bucket")
        message = 'A' * 64000
        encoded = swf.format.result(message)
        # => simpleflow+s3://jumbo-bucket/f6ea95a<...>ea3 64002

        assert encoded.startswith("simpleflow+s3://jumbo-bucket/")
        self.assertEquals(encoded.split()[1], "64002")

        key = encoded.split()[0].replace("simpleflow+s3://jumbo-bucket/", "")
        self.assertEquals(
            self.conn.get_bucket("jumbo-bucket").get_key(key).get_contents_as_string(encoding='utf-8'),
            json.dumps(message),
        )

    def test_jumbo_fields_encoding_with_directory(self):
        self.setup_jumbo_fields("jumbo-bucket/with/subdir")
        message = 'A' * 64000
        encoded = swf.format.result(message)
        # => simpleflow+s3://jumbo-bucket/with/subdir/f6ea95a<...>ea3 64002

        assert encoded.startswith("simpleflow+s3://jumbo-bucket/with/subdir/")
        self.assertEquals(encoded.split()[1], "64002")

        key = encoded.split()[0].replace("simpleflow+s3://jumbo-bucket/", "")
        self.assertEquals(
            self.conn.get_bucket("jumbo-bucket").get_key(key).get_contents_as_string(encoding='utf-8'),
            json.dumps(message),
        )

    def test_jumbo_fields_with_directory_strip_trailing_slash(self):
        self.setup_jumbo_fields("jumbo-bucket/with/subdir/")
        message = 'A' * 64000
        encoded = swf.format.result(message)

        assert not encoded.startswith("simpleflow+s3://jumbo-bucket/with/subdir//")

    def test_jumbo_fields_encoding_raise_if_encoded_form_overflows_thresholds(self):
        # 'reason' field is limited to 256 chars for instance
        self.setup_jumbo_fields("jumbo-bucket/with/a/very/long/name/" + "a" * 256)

        message = 'A' * 500
        with self.assertRaisesRegexp(ValueError, "Jumbo field signature is longer than"):
            swf.format.reason(message)
