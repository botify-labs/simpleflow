import unittest
import tempfile
import shutil
import gzip
from moto import mock_s3

from cdf.utils.s3 import *
from cdf.utils.path import partition_aware_sort


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
            'file.type.23456.gz',
            'file.type.123456789.gz',
        ]
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

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
                            regexp=['abcd', 'file.type.*'])
        expected = self.partition_files + ['file.type.gz']
        self.assertItemsEqual(expected, map(lambda i: i.name, result))

        # regexp should only be used to filter file/key's `basename`
        # it means the `bucket_uri` should be the direct parent of the files
        result = list_files('s3://', regexp=['test_bucket/file.*'])
        expected = []
        self.assertEqual(result, expected)

    @mock_s3
    def test_list_bucket_partition_order(self):
        self.setup_s3()
        file_list = list_files('s3://test_bucket',
                               regexp='file.type.[0-9]+.gz')

        sorted_list = partition_aware_sort(
            file_list,
            basename_func=lambda k: os.path.basename(k.name)
        )
        expected = self.partition_files
        self.assertListEqual(expected, map(lambda i: i.name, sorted_list))

    @mock_s3
    def test_fetch_files(self):
        self.setup_s3()
        files = fetch_files('s3://test_bucket', self.tmp_dir,
                            regexp='file.type.[0-9]+.gz')
        # assert that the return file list is usable
        for f, _ in files:
            self.assertTrue(os.path.exists(f))

        results = [os.path.basename(f) for f in os.listdir(self.tmp_dir)]
        expected = self.partition_files
        self.assertItemsEqual(expected, results)

    @mock_s3
    def test_fetch_files_non_force(self):
        self.setup_s3()
        file_name = 'file.type.gz'
        content = 'botify'
        # create a local file with different content
        with open(os.path.join(self.tmp_dir, file_name), 'w') as f:
            f.write(content)

        # check that when `force_fetch` is false, file will not be fetched
        # if the file is locally present
        files = fetch_files('s3://test_bucket', self.tmp_dir,
                            regexp=file_name, force_fetch=False)
        file, fetched = files[0]
        self.assertEqual(fetched, False)

        # check the content is not that from mocked s3
        with open(file) as f:
            data = f.read()
            self.assertEqual(data, content)

    @mock_s3
    def test_stream_s3_files(self):
        # mock a bucket with some gzipped files
        s3 = boto.connect_s3()
        test_bucket = s3.create_bucket(self.bucket)
        contents = ['line1\n', 'line2\n', 'line3\n']
        with gzip.open(os.path.join(self.tmp_dir, 't.txt.1.gz'), 'w') as f:
            f.writelines(contents)

        key = Key(test_bucket, name='t.txt.1.gz')
        key.content_encoding = 'gzip'
        key.set_contents_from_filename(f.filename)

        # check stream results
        result = list(stream_files('s3://test_bucket'))
        self.assertListEqual(result, contents)

    def test_split_lines(self):
        from cdf.utils.s3 import _split_by_lines

        # empty stream
        empty = iter('')
        result = list(_split_by_lines(empty))
        self.assertEqual(result, [])

        # regular non-empty stream
        non_empty = iter('abc\ndef\nghi\n')
        result = list(_split_by_lines(non_empty))
        expected = ['abc\n', 'def\n', 'ghi\n']
        self.assertEqual(result, expected)

        # remaining stream
        remaining = iter('abc\ndef')
        result = list(_split_by_lines(remaining))
        expected = ['abc\n', 'def\n']
        self.assertEqual(result, expected)

        # empty line
        empty_line = iter('abc\n\ndef')
        result = list(_split_by_lines(empty_line))
        expected = ['abc\n', '\n', 'def\n']
        self.assertEqual(result, expected)
