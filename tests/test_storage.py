import os
import unittest
import tempfile
import boto
from moto import mock_s3

from simpleflow import storage


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
        self.assertEquals(
            bucket.get_key("mykey.txt").get_contents_as_string(),
            "42")

    @mock_s3
    def test_push_content(self):
        self.create()
        storage.push_content(self.bucket, "mykey.txt", "Hey Jude")
        bucket = self.conn.get_bucket(self.bucket)
        self.assertEquals(
            bucket.get_key("mykey.txt").get_contents_as_string(),
            "Hey Jude")

    @mock_s3
    def test_pull(self):
        self.create()
        storage.push(self.bucket, "mykey.txt", self.tmp_filename)
        dest_tmp_filename = tempfile.mktemp()
        storage.pull(self.bucket, "mykey.txt", dest_tmp_filename)
        f = open(dest_tmp_filename)
        self.assertEquals(f.readline(), "42")

    @mock_s3
    def test_pull_content(self):
        self.create()
        storage.push(self.bucket, "mykey.txt", self.tmp_filename)
        self.assertEquals(
            storage.pull_content(self.bucket, "mykey.txt"),
            "42")

    @mock_s3
    def test_list(self):
        self.create()
        storage.push(self.bucket, "mykey.txt", self.tmp_filename)
        keys = [k for k in storage.list(self.bucket, None)]
        self.assertEquals(keys[0].key, "mykey.txt")
