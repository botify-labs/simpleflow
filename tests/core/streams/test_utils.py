# -*- coding:utf-8 -*-
import unittest
import logging
from StringIO import StringIO

from cdf.log import logger
from cdf.core.streams.utils import split_file, group_left

logger.setLevel(logging.DEBUG)


class TestUtils(unittest.TestCase):
    def test_split_file(self):
        f = StringIO()
        f.write('1\t2\n')
        f.write('3\t4\n')
        f.seek(0)

        result = list(split_file(f))
        self.assertEquals(result, [['1', '2'], ['3', '4']])


class TestGroupLeft(unittest.TestCase):
    def setUp(self):
        self.stream_1 = iter([
            (1, 'riri'),
            (2, 'fifi'),
            (3, 'loulou')
        ])
        self.stream_2 = iter([
            (1, 'donald'),
            (2, 'mickey'),
            (3, 'daisy')
        ])

    def test_harness(self):
        result = list(group_left((self.stream_1, 0),
                                 stream_2=(self.stream_2, 0)))
        expected = [
            (1, (1, 'riri'), {'stream_2': [(1, 'donald')]}),
            (2, (2, 'fifi'), {'stream_2': [(2, 'mickey')]}),
            (3, (3, 'loulou'), {'stream_2': [(3, 'daisy')]}),
        ]
        self.assertEqual(result, expected)

    def test_key(self):
        stream_1 = iter([
            (1, 2, 3),
            (1, 2, 5)
        ])
        stream_2 = iter([
            (4, 5, 3),
            (4, 5, 4)
        ])

        result = list(group_left((stream_1, 2), stream_2=(stream_2, 2)))
        expected = [
            (3, (1, 2, 3), {'stream_2': [(4, 5, 3)]}),
            (5, (1, 2, 5), {'stream_2': []})
        ]
        self.assertEqual(result, expected)

    def test_columns(self):
        content = (1, 2, 3, 4, 5, 6, 7, 8)
        stream_1 = iter([content])
        stream_2 = iter([content])

        result = list(group_left((stream_1, 0), stream_2=(stream_2, 0)))
        expected = [
            (1, content, {'stream_2': [content]})
        ]
        self.assertEqual(result, expected)

    def test_right_group(self):
        stream_1 = [[1], [2], [3], [7]]
        stream_2 = [[1], [3], [3], [5]]

        result = list(group_left((stream_1, 0), stream_2=(stream_2, 0)))
        expected = [
            (1, [1], {'stream_2': [[1]]}),
            (2, [2], {'stream_2': []}),
            (3, [3], {'stream_2': [[3], [3]]}),
            (7, [7], {'stream_2': []}),
        ]
        self.assertEqual(result, expected)

    def test_empty(self):
        # Test with an empty stream
        stream_3 = iter([])
        result = list(group_left((self.stream_1, 0),
                                 stream_2=(self.stream_2, 0),
                                 stream_3=(stream_3, 0)))
        self.assertEquals(len(result), 3)

    def test_both_skip(self):
        stream_1 = iter([[0], [2], [3], [5], [6]])
        stream_2 = iter([[2], [4], [5]])

        result = list(group_left((stream_1, 0),
                                 stream_2=(stream_2, 0)))
        expected = [
            (0, [0], {'stream_2': []}),
            (2, [2], {'stream_2': [[2]]}),
            (3, [3], {'stream_2': []}),
            (5, [5], {'stream_2': [[5]]}),
            (6, [6], {'stream_2': []}),
        ]
        self.assertEqual(result, expected)

    def test_left_skip(self):
        stream_1 = iter([
            (1, 'riri'),
            (3, 'loulou'),
            (7, 'rapetou')
        ])
        stream_2 = iter([
            (1, 'donald'),
            (2, 'mickey'),
            (3, 'daisy')
        ])

        result = list(group_left((stream_1, 0),
                                 stream_2=(stream_2, 0)))
        expected = [
            (1, (1, 'riri'), {'stream_2': [(1, 'donald')]}),
            (3, (3, 'loulou'), {'stream_2': [(3, 'daisy')]}),
            (7, (7, 'rapetou'), {'stream_2': []}),
        ]
        self.assertEqual(result, expected)

    def test_left_multiple_skip(self):
        stream_1 = iter([
            (466,),
            (467,),
            (472,),
            (474,)
        ])
        stream_2 = iter([
            (467,),
            (468,),
            (469,),
            (472,),
            (474,),
        ])

        result = list(group_left((stream_1, 0),
                                 stream_2=(stream_2, 0),
                                 stream_3=(iter([]), 0)))
        expected = [
            (466, (466,), {'stream_2': [], 'stream_3': []}),
            (467, (467,), {'stream_2': [(467,)], 'stream_3': []}),
            (472, (472,), {'stream_2': [(472,)], 'stream_3': []}),
            (474, (474,), {'stream_2': [((474,))], 'stream_3': []})
        ]
        self.assertEqual(result, expected)