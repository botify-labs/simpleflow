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
        def increment_left(attributes, stream):
            attributes['nb_left'] = attributes.get('nb_left', 0) + 1

        def increment_right(attributes, stream):
            attributes['nb_right'] = attributes.get('nb_right', 0) + 1

        result = list(group_with((self.stream_1, 0, increment_left),
                                 stream_2=(self.stream_2, 0, increment_right)))
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

        def func_left(attributes, stream):
            attributes['ok'] = True

        def skip_on_d(attributes, stream):
            if stream[1].startswith('d'):
                raise GroupWithSkipException()

        result = list(group_with((self.stream_1, 0, func_left), stream_2=(self.stream_2, 0, skip_on_d)))
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0], (2, {"ok": True}))
        self.assertEquals(result[1], (7, {"ok": True}))

    def test_empty(self):
        def increment_left(attributes, stream):
            attributes['nb_left'] = attributes.get('nb_left', 0) + 1

        def increment_stream2(attributes, stream):
            attributes['nb_stream2'] = attributes.get('nb_stream2', 0) + 1

        def increment_stream3(attributes, stream):
            attributes['nb_stream3'] = attributes.get('nb_stream3', 0) + 1

        results = list(group_with((self.stream_1, 0, increment_left),
                                  stream_2=(self.stream_2, 0, increment_stream2),
                                  stream_3=(iter([]), 0, increment_stream3)))
        for result in results:
            self.assertFalse('nb_stream3' in result)