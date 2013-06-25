# -*- coding:utf-8 -*-
import unittest
import logging
from StringIO import StringIO

from cdf.log import logger
from cdf.streams.utils import split_file, group_left

logger.setLevel(logging.DEBUG)


class TestUtils(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_split_file(self):
        f = StringIO()
        f.write('1\t2\n')
        f.write('3\t4\n')
        f.seek(0)

        result = list(split_file(f))
        self.assertEquals(result, [['1', '2'], ['3', '4']])

    def test_group_left(self):
        stream_1 = iter([
            (1, 'riri'),
            (2, 'fifi'),
            (3, 'loulou'),
            (7, 'rapetou')
        ])
        stream_2 = iter([
            (1, 'donald'),
            (3, 'daisy'),
            (3, 'picsou'),
            (5, 'geotrouvetout')
        ])

        result = list(group_left((stream_1.__iter__(), 0), stream_2=(stream_2, 0)))
        self.assertEquals(len(result), 4)
        self.assertEquals(result[0], (1, (1, 'riri'), {'stream_2': [(1, 'donald')]}))
        self.assertEquals(result[1], (2, (2, 'fifi'), {}))
        self.assertEquals(result[2], (3, (3, 'loulou'), {'stream_2': [(3, 'daisy'), (3, 'picsou')]}))
        self.assertEquals(result[3], (7, (7, 'rapetou'), {}))
