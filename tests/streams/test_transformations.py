# -*- coding:utf-8 -*-
import unittest
import logging

from cdf.log import logger
from cdf.streams.transformations import group_with, GroupWithSkipException

logger.setLevel(logging.DEBUG)


class TestGroupWith(unittest.TestCase):

    def setUp(self):
        self.stream_1 = iter([
            (1, 'riri'),
            (2, 'fifi'),
            (3, 'loulou'),
            (7, 'rapetou')
        ])
        self.stream_2 = iter([
            (1, 'donald'),
            (3, 'daisy'),
            (3, 'picsou'),
            (5, 'geotrouvetout')
        ])

    def test_group_with(self):
        def func_1(attributes, stream):
            if not 'nb_left' in attributes:
                attributes['nb_left'] = 1
            else:
                attributes['nb_left'] += 1

        def func_2(attributes, stream):
            if not 'nb_right' in attributes:
                attributes['nb_right'] = 1
            else:
                attributes['nb_right'] += 1

        result = list(group_with((self.stream_1, 0, func_1), stream_2=(self.stream_2, 0, func_2)))
        self.assertEquals(len(result), 4)
        self.assertEquals(result[0], (1, {"nb_left": 1, "nb_right": 1}))
        self.assertEquals(result[1], (2, {"nb_left": 1}))
        self.assertEquals(result[2], (3, {"nb_left": 1, "nb_right": 2}))
        # Item number 5 should not be found as in left stream, it jumps directly from 3 to 7
        self.assertEquals(result[3], (7, {"nb_left": 1}))

    def test_skip_exception(self):
        """
        We don't want to return any item in right stream starting by a "d"
        """
        def func_1(attributes, stream):
            attributes['ok'] = True

        def func_2(attributes, stream):
            if stream[1].startswith('d'):
                raise GroupWithSkipException()

        result = list(group_with((self.stream_1, 0, func_1), stream_2=(self.stream_2, 0, func_2)))
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0], (2, {"ok": True}))
        self.assertEquals(result[1], (7, {"ok": True}))

    def test_empty(self):
        def func_1(attributes, stream):
            if not 'func_1' in attributes:
                attributes['func_1'] = 1
            else:
                attributes['func_1'] += 1

        def func_2(attributes, stream):
            if not 'func_2' in attributes:
                attributes['func_2'] = 1
            else:
                attributes['func_2'] += 1

        def func_3(attributes, stream):
            if not 'func_3' in attributes:
                attributes['func_3'] = 1
            else:
                attributes['func_3'] += 1

        results = list(group_with((self.stream_1, 0, func_1),
                                 stream_2=(self.stream_2, 0, func_2),
                                 stream_3=(iter([]), 0, func_3)))
        for result in results:
            self.assertFalse('func_3' in result)