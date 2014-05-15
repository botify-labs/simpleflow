import gzip
import os
import tempfile
import unittest
import StringIO
import shutil
from mock import patch

from cdf.core.streams.base import StreamDefBase
from cdf.core.mocks import _mock_fetch_file, _mock_fetch_files


class CustomStreamDef(StreamDefBase):
    FILE = 'test'
    HEADERS = (
        ('id', int),
        ('url', str)
    )


class TestStreamsDef(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.s3_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        shutil.rmtree(self.s3_dir)

    def test_field_idx(self):
        self.assertEquals(CustomStreamDef.field_idx('id'), 0)
        self.assertEquals(CustomStreamDef.field_idx('url'), 1)

    def test_file(self):
        f = StringIO.StringIO()
        f.write('1\thttp://www.site.com/\n')
        f.write('2\thttp://www.site.com/2\n')
        f.seek(0)

        stream = CustomStreamDef.get_stream_from_file(f)
        self.assertTrue(isinstance(stream.stream_def, CustomStreamDef))
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
        """
        with gzip.open(os.path.join(self.tmp_dir, 'test.txt.0.gz'), 'w') as f:
            f.write('0\thttp://www.site.com/\n')
            f.write('1\thttp://www.site.com/1\n')
        with gzip.open(os.path.join(self.tmp_dir, 'test.txt.1.gz'), 'w') as f:
            f.write('2\thttp://www.site.com/2\n')
            f.write('3\thttp://www.site.com/3\n')
            f.write('4\thttp://www.site.com/4\n')
        with gzip.open(os.path.join(self.tmp_dir, 'test.txt.2.gz'), 'w') as f:
            f.write('5\thttp://www.site.com/5\n')
            f.write('6\thttp://www.site.com/6\n')

    def test_load_from_directory(self):
        self._write_custom_parts()
        self.assertEquals(
            list(CustomStreamDef.get_stream_from_directory(self.tmp_dir, part_id=0)),
            [
                [0, 'http://www.site.com/'],
                [1, 'http://www.site.com/1'],
            ]
        )
        self.assertEquals(
            list(CustomStreamDef.get_stream_from_directory(self.tmp_dir, part_id=1)),
            [
                [2, 'http://www.site.com/2'],
                [3, 'http://www.site.com/3'],
                [4, 'http://www.site.com/4'],
            ]
        )
        self.assertEquals(
            list(CustomStreamDef.get_stream_from_directory(self.tmp_dir, part_id=2)),
            [
                [5, 'http://www.site.com/5'],
                [6, 'http://www.site.com/6']
            ]
        )

        # Test without part_id
        self.assertEquals(
            list(CustomStreamDef.get_stream_from_directory(self.tmp_dir)),
            [
                [0, 'http://www.site.com/'],
                [1, 'http://www.site.com/1'],
                [2, 'http://www.site.com/2'],
                [3, 'http://www.site.com/3'],
                [4, 'http://www.site.com/4'],
                [5, 'http://www.site.com/5'],
                [6, 'http://www.site.com/6']
            ]
        )

    @patch('cdf.utils.s3.fetch_file', _mock_fetch_file)
    @patch('cdf.utils.s3.fetch_files', _mock_fetch_files)
    def test_load_from_s3(self):
        self._write_custom_parts()
        s3_dir = 's3://' + self.s3_dir
        self.assertEquals(
            list(CustomStreamDef.get_stream_from_s3(s3_dir, tmp_dir=self.tmp_dir, part_id=0)),
            [
                [0, 'http://www.site.com/'],
                [1, 'http://www.site.com/1'],
            ]
        )
        self.assertEquals(
            list(CustomStreamDef.get_stream_from_s3(s3_dir, tmp_dir=self.tmp_dir, part_id=1)),
            [
                [2, 'http://www.site.com/2'],
                [3, 'http://www.site.com/3'],
                [4, 'http://www.site.com/4'],
            ]
        )
        self.assertEquals(
            list(CustomStreamDef.get_stream_from_s3(s3_dir, tmp_dir=self.tmp_dir, part_id=2)),
            [
                [5, 'http://www.site.com/5'],
                [6, 'http://www.site.com/6']
            ]
        )

        # Test without part_id
        self.assertEquals(
            list(CustomStreamDef.get_stream_from_s3(s3_dir, tmp_dir=self.tmp_dir)),
            [
                [0, 'http://www.site.com/'],
                [1, 'http://www.site.com/1'],
                [2, 'http://www.site.com/2'],
                [3, 'http://www.site.com/3'],
                [4, 'http://www.site.com/4'],
                [5, 'http://www.site.com/5'],
                [6, 'http://www.site.com/6']
            ]
        )

    def test_persist(self):
        iterator = iter([
            [0, 'http://www.site.com/'],
            [1, 'http://www.site.com/1'],
            [2, 'http://www.site.com/2'],
            [3, 'http://www.site.com/3'],
            [4, 'http://www.site.com/4'],
            [5, 'http://www.site.com/5'],
            [6, 'http://www.site.com/6']
        ])
        files = CustomStreamDef().persist(
            iterator,
            self.tmp_dir,
            first_part_id_size=2,
            part_id_size=3
        )
        self.assertEquals(
            files,
            [os.path.join(self.tmp_dir, '{}.txt.{}.gz'.format(CustomStreamDef().FILE, part_id)) for part_id in xrange(0, 3)]
        )

        # Test without part_id
        self.assertEquals(
            list(CustomStreamDef.get_stream_from_directory(self.tmp_dir)),
            [
                [0, 'http://www.site.com/'],
                [1, 'http://www.site.com/1'],
                [2, 'http://www.site.com/2'],
                [3, 'http://www.site.com/3'],
                [4, 'http://www.site.com/4'],
                [5, 'http://www.site.com/5'],
                [6, 'http://www.site.com/6']
            ]
        )

    def test_temporary_dataset(self):
        dataset = CustomStreamDef.create_temporary_dataset()
        # Write in reversed to ensure that the dataset will be sorted
        for i in xrange(6, -1, -1):
            dataset.append(i, 'http://www.site.com/{}'.format(i))
        dataset.persist(self.tmp_dir, first_part_id_size=2, part_id_size=3)

        self.assertEquals(
            list(CustomStreamDef.get_stream_from_directory(self.tmp_dir)),
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
