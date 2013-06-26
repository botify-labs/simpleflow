# -*- coding:utf-8 -*-
import unittest
import logging

from cdf.log import logger
from cdf.collections.properties_stats.aggregator import PropertiesStatsAggregator, PropertiesStatsMetaAggregator

logger.setLevel(logging.DEBUG)


class TestPropertiesStats(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple(self):
        stream_patterns = iter((
            [1, 'http', 'www.site.com', '/', ''],
            [2, 'http', 'www.site.com', '/product.html', ''],
        ))

        # infos mask (2nd field) : 4 noindex, 8 nofollow
        stream_infos = iter((
            [1, 4, 'text/html', 0, 1, 200, 1200, 303, 456, True],
            [2, 8, 'text/html', 1, 1, 404, 1200, 303, 456, True],
        ))

        stream_properties = iter((
            [1, "homepage"],
            [2, "product"]
        ))

        stream_outlinks = iter((
            [1, 'a', True, 2, ''],
        ))

        a = PropertiesStatsAggregator(stream_patterns, stream_infos, stream_properties, stream_outlinks, iter([]))
        stats = a.get()
        logger.info(stats)
        # Reminder : keys = host, resource_type, content_type, depth, index, follow
        expected_keys = [
            ('www.site.com', 'homepage', 'text/html', 0, False, True),
            ('www.site.com', 'product', 'text/html', 1, True, False),
        ]

        self.assertItemsEqual(stats.keys(), expected_keys)


class TestPropertiesStatsMeta(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple(self):
        stream_patterns = iter((
            [1, 'http', 'www.site.com', '/', ''],
            [2, 'http', 'www.site.com', '/product.html', ''],
            [3, 'http', 'www.site.com', '/another_product.html', ''],
        ))

        stream_properties = iter((
            [1, "homepage"],
            [2, "product"],
            [3, "product"]
        ))

        stream_contents = iter((
            [1, 2, 1234, 'My first H1'],
            [1, 2, 456, 'My second H1'],
            [2, 2, 1234, 'My first H1'],
            [3, 2, 9877, 'My other H1']
        ))

        a = PropertiesStatsMetaAggregator(stream_patterns, stream_properties, stream_contents)

        expected_results = {
            ('www.site.com', 'product'):
                {'h1_filled_nb': 2,
                 'h1_local_unik_nb': 2,
                 'h1_global_unik_nb': 1,
                 'h2_filled_nb': 0,
                 'h2_local_unik_nb': 0,
                 'h2_global_unik_nb': 0,
                 'title_filled_nb': 0,
                 'title_global_unik_nb': 0,
                 'title_local_unik_nb': 0,
                 'description_filled_nb': 0,
                 'description_local_unik_nb': 0,
                 'description_global_unik_nb': 0,
                 },
            ('www.site.com', 'homepage'):
                {'h1_filled_nb': 1,
                 'h1_local_unik_nb': 1,
                 'h1_global_unik_nb': 0,
                 'h2_filled_nb': 0,
                 'h2_local_unik_nb': 0,
                 'h2_global_unik_nb': 0,
                 'title_filled_nb': 0,
                 'title_global_unik_nb': 0,
                 'title_local_unik_nb': 0,
                 'description_filled_nb': 0,
                 'description_local_unik_nb': 0,
                 'description_global_unik_nb': 0,
                 }
        }
        results = a.get()

        self.assertEquals(results[('www.site.com', 'product')], expected_results[('www.site.com', 'product')])
        self.assertEquals(results[('www.site.com', 'homepage')], expected_results[('www.site.com', 'homepage')])
