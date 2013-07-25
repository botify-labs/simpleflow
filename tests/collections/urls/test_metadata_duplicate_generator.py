# -*- coding:utf-8 -*-
import unittest
import logging
from datetime import datetime


from cdf.log import logger
from cdf.collections.urls.generators.metadata_duplicate import MetadataDuplicateGenerator

logger.setLevel(logging.DEBUG)


class TestMetadataDuplicateGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple(self):
        stream_patterns = iter((
            [1, 'http', 'www.site.com', '/path/name.html', ''],
            [2, 'http', 'www.site.com', '/path/name2.html', ''],
            [3, 'http', 'www.site.com', '/path/name4.html', ''],
        ))

        stream_contents = iter((
            [1, 2, 1234, 'My first H1'],
            [1, 2, 456, 'My second H1'],
            [1, 3, 7867, 'My H2'],
            [1, 1, 8999, 'My title'],
            [2, 2, 1234, 'My first H1'],
            [3, 2, 9877, 'My other H1'],
            [3, 1, 8999, 'My title'],
        ))

        u = MetadataDuplicateGenerator(stream_patterns, stream_contents)
        results = list(u)
        logger.info(results)
        expected = [
            ('title', 1, [3]),
            ('title', 3, [1]),
            ('h1', 1, [2]),
            ('h1', 2, [1])
        ]
        self.assertEquals(results, expected)
