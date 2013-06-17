# -*- coding:utf-8 -*-
import unittest
import logging
from StringIO import StringIO

from cdf.log import logger
from cdf.streams.utils import split_file

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
