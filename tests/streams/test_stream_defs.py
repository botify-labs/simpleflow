import gzip
import os
import tempfile
import unittest
import StringIO
import shutil
from mock import patch
from moto import mock_s3
import boto

from cdf.core.streams.base import StreamDefBase, Stream, TemporaryDataset
from cdf.core.mocks import _mock_fetch_file, _mock_fetch_files
from cdf.utils.s3 import list_files


class CustomStreamDef(StreamDefBase):
    FILE = 'test'
    HEADERS = (
        ('id', int),
        ('url', str)
    )


class TestStream(unittest.TestCase):
    def setUp(self):
        self.data = [
            [1, 'http://www.site.com/'],
            [2, 'http://www.site.com/2'],
            [3, 'http://www.bad.com/2']
        ]

    def test_basics(self):
        stream = Stream(CustomStreamDef, iter(self.data))
        result = list(stream)
        self.assertEqual(result, self.data)

    def test_add_filters(self):
        stream = Stream(CustomStreamDef, iter(self.data))
        stream.add_filter('url', lambda i: 'site.com' in i)
        stream.add_filter('url', lambda i: '/2' in i)
        self.assertEquals(list(stream), [[2, 'http://www.site.com/2']])


class TestStreamsDef(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.s3_dir = tempfile.mkdtemp()
        self.data = [
            [0, 'http://www.site.com/'],
            [1, 'http://www.site.com/1'],
            [2, 'http://www.site.com/2'],
            [3, 'http://www.site.com/3'],
            [4, 'http://www.site.com/4'],
            [5, 'http://www.site.com/5'],
            [6, 'http://www.site.com/6']
        ]

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        shutil.rmtree(self.s3_dir)

    def test_field_idx(self):
        self.assertEquals(CustomStreamDef.field_idx('id'), 0)
        self.assertEquals(CustomStreamDef.field_idx('url'), 1)

    def test_fields_idx(self):
        self.assertEquals(CustomStreamDef.fields_idx(['id', 'url']), [0, 1])
        #test that the function respect the input field order
        self.assertEquals(CustomStreamDef.fields_idx(['url', 'id']), [1, 0])
        #edge case, empty input field list
        self.assertEquals(CustomStreamDef.fields_idx([]), [])

    def test_file(self):
        f = StringIO.StringIO()
        f.write('1\thttp://www.site.com/\n')
        f.write('2\thttp://www.site.com/2\n')
        f.seek(0)

        stream = CustomStreamDef.get_stream_from_file(f)
        self.assertEquals(stream.next(), [1, 'http://www.site.com/'])
        self.assertEquals(stream.next(), [2, 'http://www.site.com/2'])

    def test_iterator(self):
        iterator = iter([
            [1, 'http://www.site.com/'],
            [2, 'http://www.site.com/2']
        ])
        stream = CustomStreamDef.get_stream_from_iterator(iterator)
        self.assertTrue(isinstance(stream.stream_def, CustomStreamDef))
        self.assertEquals(stream.next(), [1, 'http://www.site.com/'])
        self.assertEquals(stream.next(), [2, 'http://www.site.com/2'])

    def test_to_dict(self):
        entry = [1, 'http://www.site.com/']
        self.assertEquals(
            CustomStreamDef().to_dict(entry),
            {'id': 1, 'url': 'http://www.site.com/'}
        )

    def test_stream_filters(self):
        iterator = iter([
            [1, 'http://www.site.com/'],
            [2, 'http://www.site.com/2'],
            [3, 'http://www.bad.com/2']
        ])
        stream = CustomStreamDef.get_stream_from_iterator(iterator)
        stream.add_filter('url', lambda i: 'site.com' in i)
        stream.add_filter('url', lambda i: '/2' in i)
        self.assertEquals(list(stream), [[2, 'http://www.site.com/2']])

    def _write_custom_parts(self):
        """
        Write files mapping to a `CustomStreamDef` schema
        with `first_part_id_size` = 2, `part_id_size` = 3

        To make it trickier, partition 0, 9, 10 are written. This tests
        if the functions are partition-aware
        """
        with gzip.open(os.path.join(self.tmp_dir, 'test.txt.0.gz'), 'w') as f:
            f.write('0\thttp://www.site.com/\n')
            f.write('1\thttp://www.site.com/1\n')
        with gzip.open(os.path.join(self.tmp_dir, 'test.txt.9.gz'), 'w') as f:
            f.write('2\thttp://www.site.com/2\n')
            f.write('3\thttp://www.site.com/3\n')
            f.write('4\thttp://www.site.com/4\n')
        with gzip.open(os.path.join(self.tmp_dir, 'test.txt.10.gz'), 'w') as f:
            f.write('5\thttp://www.site.com/5\n')
            f.write('6\thttp://www.site.com/6\n')

    def test_load_from_directory(self):
        self._write_custom_parts()
        self.assertEquals(
            list(CustomStreamDef.load(self.tmp_dir, part_id=0)),
            [
                [0, 'http://www.site.com/'],
                [1, 'http://www.site.com/1'],
            ]
        )
        self.assertEquals(
            list(CustomStreamDef.load(self.tmp_dir, part_id=9)),
            [
                [2, 'http://www.site.com/2'],
                [3, 'http://www.site.com/3'],
                [4, 'http://www.site.com/4'],
            ]
        )
        self.assertEquals(
            list(CustomStreamDef.load(self.tmp_dir, part_id=10)),
            [
                [5, 'http://www.site.com/5'],
                [6, 'http://www.site.com/6']
            ]
        )

        # Test without part_id
        self.assertEquals(
            list(CustomStreamDef.load(self.tmp_dir)),
            self.data
        )

    @patch('cdf.utils.s3.fetch_file', _mock_fetch_file)
    @patch('cdf.utils.s3.fetch_files', _mock_fetch_files)
    def test_load_from_s3(self):
        self._write_custom_parts()
        s3_dir = 's3://' + self.s3_dir
        self.assertEquals(
            list(CustomStreamDef.load(s3_dir, tmp_dir=self.tmp_dir, part_id=0)),
            [
                [0, 'http://www.site.com/'],
                [1, 'http://www.site.com/1'],
            ]
        )
        self.assertEquals(
            list(CustomStreamDef.load(s3_dir, tmp_dir=self.tmp_dir, part_id=9)),
            [
                [2, 'http://www.site.com/2'],
                [3, 'http://www.site.com/3'],
                [4, 'http://www.site.com/4'],
            ]
        )
        self.assertEquals(
            list(CustomStreamDef.load(s3_dir, tmp_dir=self.tmp_dir, part_id=10)),
            [
                [5, 'http://www.site.com/5'],
                [6, 'http://www.site.com/6']
            ]
        )

        # Test without part_id
        self.assertEquals(
            list(CustomStreamDef.load(s3_dir, tmp_dir=self.tmp_dir)),
            self.data
        )

    def test_persist(self):
        iterator = iter(self.data)
        files = CustomStreamDef().persist(
            iterator,
            self.tmp_dir,
            first_part_size=2,
            part_size=3
        )
        pattern = os.path.join(self.tmp_dir, '{}.txt.{}.gz')
        self.assertEquals(
            files,
            [pattern.format(CustomStreamDef().FILE, part_id)
             for part_id in xrange(0, 3)]
        )

        # Test without part_id
        self.assertEquals(
            list(CustomStreamDef.load(self.tmp_dir)),
            self.data
        )

    def test_persist_part(self):
        stream = iter(self.data)
        part_id = 6

        CustomStreamDef.persist(
            stream,
            self.tmp_dir,
            part_id=6,
        )
        # check partition file creation
        files = os.listdir(self.tmp_dir)
        expected = ['{}.txt.{}.gz'.format(CustomStreamDef.FILE, part_id)]
        self.assertItemsEqual(files, expected)

        # check partition file content
        result = list(CustomStreamDef.load(self.tmp_dir))
        self.assertEqual(result, self.data)

    @mock_s3
    def test_persist_s3(self):
        s3 = boto.connect_s3()
        bucket = s3.create_bucket('test_bucket')
        s3_uri = 's3://test_bucket'

        stream = iter(self.data)

        CustomStreamDef.persist(
            stream,
            s3_uri,
            first_part_size=1,
            part_size=3
        )
        self.assertEqual(len(list_files(s3_uri)), 3)

        result_stream = CustomStreamDef.load(
            s3_uri,
            self.tmp_dir,
        )
        result = list(result_stream)
        self.assertEqual(result, self.data)

    @mock_s3
    def test_persist_part_s3(self):
        s3 = boto.connect_s3()
        bucket = s3.create_bucket('test_bucket')
        s3_uri = 's3://test_bucket'

        stream = iter(self.data)
        part_id = 15

        CustomStreamDef.persist(
            stream,
            s3_uri,
            part_id=part_id
        )
        self.assertEqual(len(list_files(s3_uri)), 1)

        result_stream = CustomStreamDef.load(
            s3_uri,
            self.tmp_dir,
            part_id=part_id
        )
        result = list(result_stream)
        self.assertEqual(result, self.data)


class TestTemporaryDataset(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_temporary_dataset_creation(self):
        dataset = CustomStreamDef.create_temporary_dataset()
        self.assertTrue(isinstance(dataset, TemporaryDataset))

    def test_temporary_dataset(self):
        dataset = TemporaryDataset(CustomStreamDef)
        # Write in reversed to ensure that the dataset will be sorted
        for i in xrange(6, -1, -1):
            dataset.append(i, 'http://www.site.com/{}'.format(i))
        dataset.persist(self.tmp_dir, first_part_size=2, part_size=3)

        self.assertEquals(
            list(CustomStreamDef.load(self.tmp_dir)),
            [
                [0, 'http://www.site.com/0'],
                [1, 'http://www.site.com/1'],
                [2, 'http://www.site.com/2'],
                [3, 'http://www.site.com/3'],
                [4, 'http://www.site.com/4'],
                [5, 'http://www.site.com/5'],
                [6, 'http://www.site.com/6']
            ]
        )
