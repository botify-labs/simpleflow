from __future__ import annotations

import json
import os
import random
import unittest

import boto

from simpleflow import constants, format
from simpleflow.storage import push_content
from tests.moto_compat import mock_s3


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

    @mock_s3
    def test_encode_none(self):
        self.assertEqual(format.encode(None, 1), None)

    @mock_s3
    def test_encode_smaller(self):
        MAX_LENGTH = random.randint(10, 1000)
        message = "A" * (MAX_LENGTH // 2)
        self.assertEqual(
            format.encode(message, MAX_LENGTH),
            message,
        )

    @mock_s3
    def test_encode_longer(self):
        MAX_LENGTH = random.randint(10, 1000)
        message = "A" * (MAX_LENGTH + 1)
        with self.assertRaisesRegex(ValueError, "Message too long"):
            format.encode(message, MAX_LENGTH)

    @mock_s3
    def test_identity_doesnt_use_jumbo_fields(self):
        self.setup_jumbo_fields("jumbo-bucket")
        message = "A" * (constants.MAX_RESULT_LENGTH * 2)
        with self.assertRaisesRegex(ValueError, "Message too long"):
            format.identity(message)

    @mock_s3
    def test_jumbo_fields_encoding_without_directory(self):
        self.setup_jumbo_fields("jumbo-bucket")
        message = "A" * 64000
        encoded = format.result(message)
        # => simpleflow+s3://jumbo-bucket/f6ea95a<...>ea3 64002

        assert encoded.startswith("simpleflow+s3://jumbo-bucket/")
        self.assertEqual(encoded.split()[1], "64002")

        key = encoded.split()[0].replace("simpleflow+s3://jumbo-bucket/", "")
        self.assertEqual(
            self.conn.get_bucket("jumbo-bucket").get_key(key).get_contents_as_string(encoding="utf-8"),
            json.dumps(message),
        )

    @mock_s3
    def test_jumbo_fields_encoding_with_directory(self):
        self.setup_jumbo_fields("jumbo-bucket/with/subdir")
        message = "A" * 64000
        encoded = format.result(message)
        # => simpleflow+s3://jumbo-bucket/with/subdir/f6ea95a<...>ea3 64002

        assert encoded.startswith("simpleflow+s3://jumbo-bucket/with/subdir/")
        self.assertEqual(encoded.split()[1], "64002")

        key = encoded.split()[0].replace("simpleflow+s3://jumbo-bucket/", "")
        self.assertEqual(
            self.conn.get_bucket("jumbo-bucket").get_key(key).get_contents_as_string(encoding="utf-8"),
            json.dumps(message),
        )

    @mock_s3
    def test_jumbo_fields_with_directory_strip_trailing_slash(self):
        self.setup_jumbo_fields("jumbo-bucket/with/subdir/")
        message = "A" * 64000
        encoded = format.result(message)

        assert not encoded.startswith("simpleflow+s3://jumbo-bucket/with/subdir//")

    @mock_s3
    def test_jumbo_fields_encoding_raise_if_encoded_form_overflows_thresholds(self):
        # 'reason' field is limited to 256 chars for instance
        self.setup_jumbo_fields("jumbo-bucket/with/a/very/long/name/" + "a" * 256)

        message = "A" * 500
        with self.assertRaisesRegex(ValueError, "Jumbo field signature is longer than"):
            format.reason(message)

    @mock_s3
    def test_decode(self):
        self.setup_jumbo_fields("jumbo-bucket")
        push_content("jumbo-bucket", "abc", "decoded jumbo field yay!")

        cases = [
            [None, None],
            ["foo bar baz", "foo bar baz"],
            ['"a string"', "a string"],
            ["[1, 2]", [1, 2]],
            ["simpleflow+s3://jumbo-bucket/abc 24", "decoded jumbo field yay!"],
        ]

        for case in cases:
            self.assertEqual(case[1], format.decode(case[0]))

    @mock_s3
    def test_decode_no_parse_json(self):
        self.setup_jumbo_fields("jumbo-bucket")
        push_content("jumbo-bucket", "abc", "decoded jumbo field yay!")

        cases = [
            [None, None],
            ["foo bar baz", "foo bar baz"],
            ['"a string"', '"a string"'],
            ["[1, 2]", "[1, 2]"],
            ["simpleflow+s3://jumbo-bucket/abc 24", "decoded jumbo field yay!"],
        ]

        for case in cases:
            self.assertEqual(case[1], format.decode(case[0], parse_json=False))
