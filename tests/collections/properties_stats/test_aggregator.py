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
            [2, 'canonical', True, 1, ''],
        ))

        stream_inlinks = iter((
            [1, 'canonical', True, 2],
            [2, 'a', True, 1]
        ))

        a = PropertiesStatsAggregator(stream_patterns, stream_infos, stream_properties, stream_outlinks, stream_inlinks)
        stats = a.get()
        logger.info(stats)

        # Reminder : keys = host, resource_type, content_type, depth, index, follow
        expected_keys = [
            ('www.site.com', 'homepage', 'text/html', 0, False, True),
            ('www.site.com', 'product', 'text/html', 1, True, False),
        ]

        self.assertItemsEqual(stats.keys(), expected_keys)
        homepage_key = ('www.site.com', 'homepage', 'text/html', 0, False, True)
        self.assertEquals(stats[homepage_key]['pages_nb'], 1)
        self.assertEquals(stats[homepage_key]['pages_code_ok'], 1)
        self.assertEquals(stats[homepage_key]['pages_code_ko'], 0)
        self.assertEquals(stats[homepage_key]['outlinks_nb'], 1)
        self.assertEquals(stats[homepage_key]['canonical_incoming_nb'], 1)

        product_key = ('www.site.com', 'product', 'text/html', 1, True, False)
        self.assertEquals(stats[product_key]['pages_nb'], 1)
        self.assertEquals(stats[product_key]['pages_code_ok'], 0)
        self.assertEquals(stats[product_key]['pages_code_ko'], 1)
        self.assertEquals(stats[product_key]['inlinks_nb'], 1)
        self.assertEquals(stats[product_key]['inlinks_follow_nb'], 1)
        self.assertEquals(stats[product_key]['inlinks_nofollow_nb'], 0)
        self.assertEquals(stats[product_key]['outlinks_nb'], 0)
        self.assertEquals(stats[product_key]['canonical_filled_nb'], 1)
        self.assertEquals(stats[product_key]['canonical_duplicates_nb'], 1)


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
            [1, 3, 7867, 'My H2'],
            [1, 1, 8999, 'My title'],
            [2, 2, 1234, 'My first H1'],
            [3, 2, 9877, 'My other H1'],
            [3, 3, 7867, 'My H2'],
        ))

        a = PropertiesStatsMetaAggregator(stream_patterns, stream_properties, stream_contents)

        expected_results = {
            ('www.site.com', 'product'):
                {'h1_filled_nb': 2,
                 'h1_local_unik_nb': 2,
                 'h1_global_unik_nb': 1,
                 'h2_filled_nb': 1,
                 'h2_local_unik_nb': 1,
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
                 'h2_filled_nb': 1,
                 'h2_local_unik_nb': 1,
                 'h2_global_unik_nb': 0,
                 'title_filled_nb': 1,
                 'title_global_unik_nb': 1,
                 'title_local_unik_nb': 1,
                 'description_filled_nb': 0,
                 'description_local_unik_nb': 0,
                 'description_global_unik_nb': 0,
                 }
        }
        results = a.get()

        self.assertEquals(results[('www.site.com', 'product')], expected_results[('www.site.com', 'product')])
        self.assertEquals(results[('www.site.com', 'homepage')], expected_results[('www.site.com', 'homepage')])
