# coding=utf-8
import unittest
import tempfile
import os
import shutil
import gzip
from StringIO import StringIO

from cdf.utils.path import group_by_part, write_by_part, utf8_writer, utf8_reader


def _simple_to_string(row):
    return '\t'.join(str(field) for field in row) + '\n'


class TestPath(unittest.TestCase):
    def setUp(self):
        self.data = [
            [4, 301],
            [8, 200],
            [9, 500],
            [14, 404],
            [15, 200],
            [1000, 304]  # should be in part 250
        ]

        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        # delete tmp dir created
        shutil.rmtree(self.tmp_dir)

    def test_group_by_part(self):
        res = [(gid, list(group)) for
               gid, group in group_by_part(iter(self.data), 2, 4)]

        expected = [
            (1, [[4, 301]]),
            (2, [[8, 200], [9, 500]]),
            (4, [[14, 404], [15, 200]]),
            (250, [[1000, 304]])
        ]

        self.assertEqual(res, expected)

    def test_write_by_part(self):
        file_pattern = 'test_{}'
        write_by_part(iter(self.data), 2, 4,
                      self.tmp_dir, file_pattern,
                      _simple_to_string)

        # verifies file creation
        files_created = os.listdir(self.tmp_dir)
        expected = [file_pattern.format(i) for i in (1, 2, 4, 250)]
        self.assertItemsEqual(files_created, expected)

        # verifies file contents
        with gzip.open(os.path.join(self.tmp_dir, 'test_1')) as f:
            lines = f.readlines()
            self.assertEqual(lines, ['4\t301\n'])

        with gzip.open(os.path.join(self.tmp_dir, 'test_2')) as f:
            lines = f.readlines()
            self.assertEqual(lines, ['8\t200\n', '9\t500\n'])

        with gzip.open(os.path.join(self.tmp_dir, 'test_4')) as f:
            lines = f.readlines()
            self.assertEqual(lines, ['14\t404\n', '15\t200\n'])

        with gzip.open(os.path.join(self.tmp_dir, 'test_250')) as f:
            lines = f.readlines()
            self.assertEqual(lines, ['1000\t304\n'])

    def test_write_empty(self):
        file_pattern = 'test_{}'
        # generator is empty
        write_by_part(iter([]), 2, 4,
                      self.tmp_dir, file_pattern,
                      _simple_to_string)
        files_created = os.listdir(self.tmp_dir)

        # nothing should be created and no exceptions
        self.assertEqual(files_created, [])

    def test_utf8_read_write(self):
        file = StringIO()

        french = u"ùûüÿ€àâæçéèêëïîôœ\n"
        chinese = u"你好我是程序员\n"

        writer = utf8_writer(file)
        writer.write(french)
        writer.write(chinese)

        file.seek(0)

        reader = utf8_reader(file)
        line1 = reader.readline()
        line2 = reader.readline()
        reader.close()

        self.assertEqual(french, line1)
        self.assertEqual(chinese, line2)
        self.assertFalse(french is line1)
        self.assertFalse(chinese is line2)