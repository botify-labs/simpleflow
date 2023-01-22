from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

import boto

from simpleflow import storage
from tests.moto_compat import mock_s3


# disable storage.BUCKET_LOCATIONS_CACHE because it interfers with tests
class DevNullCache(dict):
    def __setitem__(self, key, value):
        pass


storage.BUCKET_LOCATIONS_CACHE = DevNullCache()


class TestGroup(unittest.TestCase):
    def create(self):
        self.bucket = "bucket"
        self.conn = boto.connect_s3()
        self.conn.create_bucket(self.bucket)

    def setUp(self):
        self.tmp_filename = tempfile.mktemp()
        f = open(self.tmp_filename, "w")
        f.write("42")
        f.close()

    def tearDown(self):
        os.remove(self.tmp_filename)

    @mock_s3
    def test_push_file(self):
        self.create()
        storage.push(self.bucket, "mykey.txt", self.tmp_filename)
        bucket = self.conn.get_bucket(self.bucket)
        self.assertEqual(bucket.get_key("mykey.txt").get_contents_as_string(encoding="utf-8"), "42")

    @mock_s3
    def test_push_content(self):
        self.create()
        storage.push_content(self.bucket, "mykey.txt", "Hey Jude")
        bucket = self.conn.get_bucket(self.bucket)
        self.assertEqual(
            bucket.get_key("mykey.txt").get_contents_as_string(encoding="utf-8"),
            "Hey Jude",
        )

    @mock_s3
    def test_pull(self):
        self.create()
        storage.push(self.bucket, "mykey.txt", self.tmp_filename)
        dest_tmp_filename = tempfile.mktemp()
        storage.pull(self.bucket, "mykey.txt", dest_tmp_filename)
        f = open(dest_tmp_filename)
        self.assertEqual(f.readline(), "42")

    @mock_s3
    def test_pull_content(self):
        self.create()
        storage.push(self.bucket, "mykey.txt", self.tmp_filename)
        self.assertEqual(storage.pull_content(self.bucket, "mykey.txt"), "42")

    @mock_s3
    def test_list(self):
        self.create()
        storage.push(self.bucket, "mykey.txt", self.tmp_filename)
        keys = [k for k in storage.list_keys(self.bucket, None)]
        self.assertEqual(keys[0].key, "mykey.txt")

    @mock_s3
    def test_sanitize_bucket_and_host(self):
        self.create()

        # bucket where "get_location" works: return bucket+region
        self.assertEqual(storage.sanitize_bucket_and_host(self.bucket), (self.bucket, "us-east-1"))

        # bucket where "get_location" doesn't work: return bucket + default region setting
        def _access_denied():
            from boto.exception import S3ResponseError

            err = S3ResponseError("reason", "resp")
            err.error_code = "AccessDenied"
            raise err

        with patch("boto.s3.bucket.Bucket.get_location", side_effect=_access_denied):
            with patch("simpleflow.settings.SIMPLEFLOW_S3_HOST") as default:
                self.assertEqual(
                    storage.sanitize_bucket_and_host(self.bucket),
                    (self.bucket, default),
                )

        # bucket where we provided a host/bucket: return bucket+host
        self.assertEqual(
            storage.sanitize_bucket_and_host(f"s3.amazonaws.com/{self.bucket}"),
            (self.bucket, "s3.amazonaws.com"),
        )

        # bucket trivially invalid: raise
        with self.assertRaises(ValueError):
            storage.sanitize_bucket_and_host("any/mybucket")

        # bucket with too many "/": raise
        with self.assertRaises(ValueError):
            storage.sanitize_bucket_and_host("s3-eu-west-1.amazonaws.com/mybucket/subpath")
