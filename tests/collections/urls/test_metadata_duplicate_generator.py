# -*- coding:utf-8 -*-
import unittest
import logging
from datetime import datetime


from cdf.log import logger
from cdf.collections.urls.transducers.metadata_duplicate import get_duplicate_metadata

logger.setLevel(logging.DEBUG)


class TestMetadataDuplicateGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple(self):
        stream_contents = iter((
            [1, 2, 1234, 'My first H1'],
            [1, 2, 456, 'My second H1'],
            [1, 3, 7867, 'My H2'],
            [1, 1, 8999, 'My title'],
            [1, 4, 1111, 'My Desc'],
            [2, 2, 1234, 'My first H1'],
            [2, 4, 1111, 'My Desc'],
            [3, 2, 456, 'My second H1'],
            [3, 1, 8999, 'My title'],
            [3, 4, 1111, 'My Desc'],
        ))

        generator = get_duplicate_metadata(stream_contents)
        results = list(generator)
        logger.info(results)
        expected = [
            (1, 1, 1, 2, True, [3]),
            (1, 2, 2, 2, True, [2]),
            (1, 4, 1, 3, True, [2, 3]),
            (2, 2, 1, 2, False, [1]),
            (2, 4, 1, 3, False, [1, 3]),
            (3, 1, 1, 2, False, [1]),
            (3, 4, 1, 3, False, [1, 2])
        ]
        self.assertEquals(results, expected)
