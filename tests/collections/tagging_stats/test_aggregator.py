# -*- coding:utf-8 -*-
import unittest
import logging

from cdf.log import logger
from cdf.collections.tagging_stats.aggregator import (MetricsAggregator, MetadataAggregator,
                                                      MetricsConsolidator)

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
            [1, 'a', 'follow', 2, ''],
            [2, 'canonical', 'follow', 1, ''],
        ))

        stream_inlinks = iter((
            [1, 'canonical', 'follow', 2],
            [2, 'a', 'follow', 1]
        ))

        a = MetricsAggregator(stream_patterns, stream_infos, stream_properties, stream_outlinks, stream_inlinks)
        stats = a.get()
        logger.info(stats)

        # Reminder : keys = host, resource_type, content_type, depth, http_code, index, follow
        expected_keys = [
            ['www.site.com', 'homepage', 'text/html', 0, 200, False, True],
            ['www.site.com', 'product', 'text/html', 1, 404, True, False],
        ]
        cross_properties = [k['cross_properties'] for k in stats]
        self.assertItemsEqual(cross_properties, expected_keys)

        homepage_idx = cross_properties.index(['www.site.com', 'homepage', 'text/html', 0, 200, False, True])
        stats_homepage = stats[homepage_idx]['counters']
        self.assertEquals(stats_homepage['pages_nb'], 1)
        self.assertEquals(stats_homepage['outlinks_nb'], 1)
        self.assertEquals(stats_homepage['canonical_incoming_nb'], 1)

        product_idx = cross_properties.index(['www.site.com', 'product', 'text/html', 1, 404, True, False])
        stats_product = stats[product_idx]['counters']
        self.assertEquals(stats_product['pages_nb'], 1)
        self.assertEquals(stats_product['inlinks_nb'], 1)
        self.assertEquals(stats_product['inlinks_follow_nb'], 1)
        self.assertEquals(stats_product['inlinks_link_nofollow_nb'], 0)
        self.assertEquals(stats_product['inlinks_meta_nofollow_nb'], 0)
        self.assertEquals(stats_product['outlinks_nb'], 0)
        self.assertEquals(stats_product['canonical_filled_nb'], 1)
        self.assertEquals(stats_product['canonical_duplicates_nb'], 1)


class TestMetricsConsolidator(unittest.TestCase):

    def test_simple(self):
        stats_part_0 = [
            {
                "cross_properties": ["www.site.com", "homepage", "text/html", 0, 200, True, True],
                "counters": {
                    "pages_nb": 10,
                }
            },
            {
                "cross_properties": ["my.site.com", "product", "text/html", 0, 301, True, True],
                "counters": {
                    "pages_nb": 30,
                }
            },
            {
                "cross_properties": ["my.site.com", "product", "text/html", 0, 200, True, True],
                "counters": {
                    "pages_nb": 10,
                }
            }
        ]

        stats_part_1 = [
            {
                "cross_properties": ["my.site.com", "product", "text/html", 0, 200, True, True],
                "counters": {
                    "pages_nb": 10,
                }
            },
            {
                "cross_properties": ["music.site.com", "artist", "text/html", 0, 404, True, True],
                "counters": {
                    "pages_nb": 30,
                }
            }
        ]

        stats_part_2 = [
            {
                "cross_properties": ["music.site.com", "artist", "text/html", 0, 200, True, True],
                "counters": {
                    "pages_nb": 130,
                }
            }
        ]

        c = MetricsConsolidator([stats_part_0, stats_part_1, stats_part_2])
        aggregated_data = c.consolidate()

        expected_data = {
            ('music.site.com', 'artist', 'text/html', 0, 200, True, True): {
                'pages_nb': 130,
            },
            ('music.site.com', 'artist', 'text/html', 0, 404, True, True): {
                'pages_nb': 30,
            },
            ('www.site.com', 'homepage', 'text/html', 0, 200, True, True): {
                'pages_nb': 10,
            },
            ('my.site.com', 'product', 'text/html', 0, 200, True, True): {
                'pages_nb': 20,
            },
            ('my.site.com', 'product', 'text/html', 0, 301, True, True): {
                'pages_nb': 30,
            }
        }

        for key, value in expected_data.iteritems():
            self.assertEquals(aggregated_data[key], value)


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

        stream_infos = iter((
            [1, 4, 'text/html', 0, 1, 200, 1200, 303, 456, True],
            [2, 8, 'text/html', 1, 1, 200, 1200, 303, 456, True],
            [3, 8, 'text/html', 1, 1, 200, 1200, 303, 456, True],
        ))

        a = MetadataAggregator(stream_patterns, stream_properties, stream_contents, stream_infos)

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
                 'not_enough_metadata': 2
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
                 'not_enough_metadata': 1,
                 }
        }
        results = a.get()

        self.assertEquals(results[('www.site.com', 'product')], expected_results[('www.site.com', 'product')])
        self.assertEquals(results[('www.site.com', 'homepage')], expected_results[('www.site.com', 'homepage')])

    def test_not_enugh_metadata_bad_code(self):
        """
        A page with code not in (200, 304) should not be returned with "not_enough_metadata"
        """
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
        ))

        stream_infos = iter((
            [1, 4, 'text/html', 0, 1, 200, 1200, 303, 456, True], # 200 code
            [2, 8, 'text/html', 1, 1, 200, 1200, 303, 456, True], # 200 code
            [3, 8, 'text/html', 1, 1, 301, 1200, 303, 456, True], # 301 code
        ))

        a = MetadataAggregator(stream_patterns, stream_properties, stream_contents, stream_infos)
        results = a.get()
        self.assertEquals(results[('www.site.com', 'product')]['not_enough_metadata'], 1)


    def test_metadata(self):
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
            [1, 4, 999, 'My description'],
            [1, 1, 8999, 'My title'],
            [2, 2, 1234, 'My first H1'],
            [3, 2, 9877, 'My other H1'],
            [3, 3, 7867, 'My H2'],
            [3, 1, 78867, 'My title'],
            [3, 4, 3999, 'My description'],
        ))

        stream_infos = iter((
            [1, 4, 'text/html', 0, 1, 200, 1200, 303, 456, True],
            [2, 8, 'text/html', 1, 1, 200, 1200, 303, 456, True],
            [3, 8, 'text/html', 1, 1, 200, 1200, 303, 456, True],
        ))

        a = MetadataAggregator(stream_patterns, stream_properties, stream_contents, stream_infos)
        results = a.get()
        self.assertEquals(results[('www.site.com', 'homepage')]['not_enough_metadata'], 0)
        self.assertEquals(results[('www.site.com', 'product')]['not_enough_metadata'], 1)
