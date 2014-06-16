import unittest
import tempfile
import shutil
from moto import mock_s3
import boto
from threading import Lock

from cdf.utils.s3 import *


class TestS3Module(unittest.TestCase):
    def setUp(self):
        self.bucket = 'test_bucket'
        self.files = [
            'file.type.9.gz',
            'file.type.11.gz',
            'file.type.123456789.gz',
            'file.type.23456.gz',
            'file_other',
            'file.type.gz'
        ]
        self.partition_files = [
            'file.type.9.gz',
            'file.type.11.gz',
            'file.type.123456789.gz',
            'file.type.23456.gz',
        ]

    def setup_s3(self):
        s3 = boto.connect_s3()
        test_bucket = s3.create_bucket(self.bucket)
        test_contents = self.files

        # init files within the mock test bucket
        # file content are idem as the file name
        for content in test_contents:
            key = Key(test_bucket)
            key.name = content
            key.set_contents_from_string(content)

        return s3

    def test_uri_parse(self):
        bucket, location = uri_parse('s3://bucket/location/sublocation')
        self.assertEquals(bucket, 'bucket')
        self.assertEquals(location, 'location/sublocation')

    @mock_s3
    def test_list_bucket(self):
        self.setup_s3()
        bucket_uri = 's3://test_bucket'

        # default list bucket
        result = list_files(bucket_uri)
        expected = self.files
        self.assertItemsEqual(expected, map(lambda i: i.name, result))

        # list bucket with a regexp string
        result = list_files(bucket_uri, regexp='file.type.[0-9]+.gz')
        expected = self.partition_files
        self.assertItemsEqual(expected, map(lambda i: i.name, result))

        # list bucket with a list of regexp string
        result = list_files(bucket_uri,
                            regexp=['files.type.[0-9]+.gz', 'file.type.*'])
        expected = self.partition_files + ['file.type.gz']
        self.assertItemsEqual(expected, map(lambda i: i.name, result))

    def test_list_bucket_partition_order(self):
        pass

    @mock_s3
    def test_fetch_files(self):
        self.setup_s3()
        tmp_dir = tempfile.mkdtemp()
        files = fetch_files('s3://test_bucket', tmp_dir,
                            regexp='file.type.[0-9]+.gz')
        # assert that the return file list is usable
        for f, _ in files:
            self.assertTrue(os.path.exists(f))

        results = [os.path.basename(f) for f in os.listdir(tmp_dir)]
        expected = self.partition_files
        self.assertItemsEqual(expected, results)
        shutil.rmtree(tmp_dir)

    @mock_s3
    def test_fetch_files_non_force(self):
        self.setup_s3()
        file_name = 'file.type.gz'
        content = 'botify'
        tmp_dir = tempfile.mkdtemp()
        # create a local file with different content
        f = open(os.path.join(tmp_dir, file_name), 'w')
        f.write(content)
        f.close()

        # check that when `force_fetch` is false, file will not be fetched
        # if the file is locally present
        files = fetch_files('s3://test_bucket', tmp_dir,
                            regexp=file_name, force_fetch=False)
        file, fetched = files[0]
        self.assertEqual(fetched, False)

        # check the content is not that from mocked s3
        f = open(file)
        data = f.read()
        self.assertEqual(data, content)
        f.close()
        shutil.rmtree(tmp_dir)