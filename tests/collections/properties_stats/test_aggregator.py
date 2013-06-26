# -*- coding:utf-8 -*-
import unittest
import logging

from cdf.log import logger
from cdf.collections.properties_stats.aggregator import PropertiesStatsAggregator

logger.setLevel(logging.DEBUG)


class TestUrlDataGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple(self):
        stream_patterns = iter((
            [1, 'http', 'www.site.com', '/', ''],
            [2, 'http', 'www.site.com', '/product.html', ''],
        ))

        stream_infos = iter((
            [1, 0, 'text/html', 0, 1, 200, 1200, 303, 456, True],
            [2, 0, 'text/html', 1, 1, 404, 1200, 303, 456, True],
        ))

        stream_properties = iter((
            [1, "homepage"],
            [2, "product"]
        ))

        stream_outlinks = iter((
            [1, 'a', True, 2, ''],
        ))

        stream_contents = iter((
            [1, 2, 0, 'My first H1'],
            [1, 2, 0, 'My second H1'],
            [1, 1, 0, 'My title']
        ))

        a = PropertiesStatsAggregator(stream_patterns, stream_infos, stream_properties, stream_outlinks, iter([]), stream_contents)
        stats = a.get()
        logger.info(stats)
        # Reminder : keys = host, resource_type, content_type, depth, index, follow
        expected_keys = [
            ('www.site.com', 'homepage', 'text/html', 0, True, True),
            ('www.site.com', 'product', 'text/html', 1, True, True),
        ]
        self.assertEquals(stats.keys(), expected_keys)
