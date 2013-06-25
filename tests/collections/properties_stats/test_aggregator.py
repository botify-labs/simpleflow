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
            [1, 0, 1, 200, 1200, 303, 456, True],
            [2, 1, 1, 404, 1200, 303, 456, True],
        ))

        stream_properties = iter((
            [1, "homepage"],
            [2, "product"]
        ))

        a = PropertiesStatsAggregator(stream_patterns, stream_infos, stream_properties)
        stats = a.get()
        print stats
