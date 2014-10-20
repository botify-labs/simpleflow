import unittest
import boto
from moto import mock_s3
from cdf.exceptions import MalformedFileNameError
from cdf.utils.remote_files import (
    enumerate_partitions,
    get_part_id_from_filename
)


class TestEnumeratePartitions(unittest.TestCase):

    def _get_bucket(self):
        bucket = 'test_bucket'
        s3 = boto.connect_s3()
        test_bucket = s3.create_bucket(bucket)
        return test_bucket

    def _create_file(self, bucket, file_name):
        key = boto.s3.key.Key(bucket)
        key.name = file_name
        key.set_contents_from_string("")

    @mock_s3
    def test_nominal_case(self):
        test_bucket = self._get_bucket()
        self._create_file(test_bucket, "urlids.txt.0.gz")
        self._create_file(test_bucket, "urlids.txt.4.gz")

        self.assertEquals([0, 4], enumerate_partitions("s3://test_bucket/"))

    @mock_s3
    def test_misc_files(self):
        test_bucket = self._get_bucket()
        self._create_file(test_bucket, "urlids.txt.0.gz")
        self._create_file(test_bucket, "urlinfos.txt.4.gz")

        self.assertEquals([0], enumerate_partitions("s3://test_bucket/"))

    @mock_s3
    def test_regex_special_character(self):
        test_bucket = self._get_bucket()
        #check that "." in regex is not interpret as wildcard
        self._create_file(test_bucket, "urlids_txt_0_gz")
        #check that only files ending with the patterns are considered
        self._create_file(test_bucket, "urlids_txt_0_gz.foo")

        self.assertEquals([], enumerate_partitions("s3://test_bucket/"))


class TestGetPartIdFromFileName(unittest.TestCase):
    def test_nominal_case(self):
        self.assertEqual(0, get_part_id_from_filename("urlcontents.txt.0.gz"))
        self.assertEqual(10, get_part_id_from_filename("urlcontents.txt.10.gz"))
        self.assertEqual(0, get_part_id_from_filename("/tmp/urlcontents.txt.0.gz"))

    def test_malformed_filename(self):
        self.assertRaises(MalformedFileNameError,
                          get_part_id_from_filename,
                          "urlcontents.txt.gz")

        self.assertRaises(MalformedFileNameError,
                          get_part_id_from_filename,
                          "urlcontents.txt.-1.gz")


