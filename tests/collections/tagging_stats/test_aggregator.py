# -*- coding:utf-8 -*-
import unittest
import logging

from cdf.log import logger
from cdf.collections.tagging_stats.aggregator import (MetricsAggregator, MetadataAggregator,
                                                      MetricsConsolidator)
from cdf.utils.dict import flatten_dict
logger.setLevel(logging.DEBUG)


def reverse_outlinks(stream_outlinks):
    stream_inlinks = []
    for k in stream_outlinks:
        stream_inlinks.append([k[3], k[1], k[2], k[0]])
    return filter(lambda i: i[0] > 0, sorted(stream_inlinks, key=lambda i: i[0]))


class TestTaggingStats(unittest.TestCase):

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

        stream_outlinks = (
            [1, 'a', ['follow'], 2, ''],
            [1, 'a', ['follow'], 2, ''],
            [2, 'a', ['follow'], 1, ''],
            [2, 'a', ['link'], 1, ''],
            [2, 'a', ['link', 'robots'], 3, ''],
            [2, 'a', ['link', 'meta'], 4, ''],
            [2, 'canonical', ['follow'], 1, ''],
        )

        stream_inlinks = reverse_outlinks(stream_outlinks)

        a = MetricsAggregator(stream_patterns, stream_infos, stream_properties, iter(stream_outlinks), iter(stream_inlinks))
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
        self.assertEquals(
            stats_homepage['outlinks_internal_nb'],
            {
                'total': 2,
                'nofollow': 0,
                'follow': 2,
                'follow_unique': 1,
                'nofollow_combinations': {
                    'link': 0,
                    'link_meta': 0,
                    'link_meta_robots': 0,
                    'link_robots': 0,
                    'meta': 0,
                    'meta_robots': 0,
                    'robots': 0
                }
            }
        )
        self.assertEquals(
            stats_homepage['inlinks_internal_nb'],
            {
                'total': 2,
                'follow': 1,
                'follow_unique': 1,
                'nofollow': 1,
                'nofollow_combinations': {
                    'link': 1,
                    'link_meta': 0,
                    'link_meta_robots': 0,
                    'link_robots': 0,
                    'meta': 0,
                    'meta_robots': 0,
                    'robots': 0
                }
            }
        )
        self.assertEquals(stats_homepage['canonical_incoming_nb'], 1)

        product_idx = cross_properties.index(['www.site.com', 'product', 'text/html', 1, 404, True, False])
        stats_product = stats[product_idx]['counters']
        self.assertEquals(stats_product['pages_nb'], 1)
        self.assertEquals(stats_product['inlinks_internal_nb']['follow'], 2)
        self.assertEquals(stats_product['outlinks_internal_nb']['total'], 4)
        self.assertEquals(stats_product['outlinks_internal_nb']['follow'], 1)
        self.assertEquals(stats_product['outlinks_internal_nb']['nofollow'], 3)
        self.assertEquals(
            stats_product['outlinks_internal_nb']['nofollow_combinations'],
            {
                'link_meta': 1,
                'link_meta_robots': 0,
                'link_robots': 1,
                'link': 1,
                'meta': 0,
                'meta_robots': 0,
                'robots': 0
            }
        )
        self.assertEquals(stats_product['canonical_filled_nb'], 1)
        self.assertEquals(stats_product['canonical_duplicates_nb'], 1)

    def test_links(self):
        stream_patterns = iter((
            [1, 'http', 'www.site.com', '/', ''],
            [2, 'http', 'www.site.com', '/product1.html', ''],
            [3, 'http', 'www.site.com', '/product2.html', ''],
            [4, 'http', 'www.site.com', '/product3.html', ''],
        ))

        # infos mask (2nd field) : 4 noindex, 8 nofollow
        stream_infos = iter((
            [1, 4, 'text/html', 0, 1, 200, 1200, 303, 456, True],
            [2, 8, 'text/html', 1, 1, 200, 1200, 303, 456, True],
            [3, 8, 'text/html', 1, 1, 200, 1200, 303, 456, True],
            [4, 8, 'text/html', 1, 1, 200, 1200, 303, 456, True],
        ))

        stream_properties = iter((
            [1, "homepage"],
            [2, "product"],
            [3, "product"],
            [4, "product"]
        ))

        stream_outlinks = (
            [1, 'a', ['follow'], 2, ''],
            [1, 'a', ['follow'], 2, ''],
            [1, 'a', ['follow'], 4, ''],
            [1, 'a', ['follow'], 3, ''],
            [1, 'a', ['follow'], 3, ''],
            [1, 'a', ['follow'], 4, ''],
            [1, 'a', ['follow'], -1, 'http://www.youtube.com/'],
            [2, 'a', ['meta', 'link'], 3, ''],
            [2, 'a', ['meta'], 4, ''],
            [2, 'a', ['meta'], 1, ''],
            [2, 'a', ['meta', 'link'], 1, ''],
        )

        stream_inlinks = reverse_outlinks(stream_outlinks)

        a = MetricsAggregator(stream_patterns, stream_infos, stream_properties, iter(stream_outlinks), iter(stream_inlinks))
        stats = a.get()
        cross_properties = [k['cross_properties'] for k in stats]

        product_idx = cross_properties.index(['www.site.com', 'product', 'text/html', 1, 200, True, False])
        stats_product = stats[product_idx]['counters']

        self.assertEquals(stats_product['pages_nb'], 3)
        self.assertEquals(stats_product['inlinks_internal_nb']['follow'], 6)
        self.assertEquals(stats_product['inlinks_internal_nb']['follow_unique'], 3)
        self.assertEquals(stats_product['inlinks_internal_nb']['nofollow'], 2)
        self.assertEquals(stats_product['inlinks_internal_nb']['nofollow_combinations']['link_meta'], 1)
        self.assertEquals(stats_product['inlinks_internal_nb']['nofollow_combinations']['meta'], 1)
        self.assertEquals(stats_product['outlinks_internal_nb']['nofollow'], 4)
        self.assertEquals(stats_product['outlinks_internal_nb']['nofollow_combinations']['link_meta'], 2)
        self.assertEquals(stats_product['outlinks_internal_nb']['nofollow_combinations']['meta'], 2)


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
                    "inlinks_internal_nb": {
                        "total": 1,
                        "nofollow": 1,
                        "follow": 0,
                        "nofollow_combinations": {
                            "link_meta": 1
                        }
                    }
                }
            }
        ]

        stats_part_1 = [
            {
                "cross_properties": ["my.site.com", "product", "text/html", 0, 200, True, True],
                "counters": {
                    "pages_nb": 12,
                    "inlinks_internal_nb": {
                        "total": 11,
                        "nofollow": 11,
                        "follow": 0,
                        "nofollow_combinations": {
                            "link_meta": 1,
                            "link": 10
                        }
                    }
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

        metadata_only_part = [
            {
                "cross_properties": ['music.site.com', 'artist', 'text/html', 0, 200, True, True],
                "counters": {
                    "metadata_nb": {
                        "h1": {
                            "filled": 100,
                            "unique": 90
                        }
                    }
                }
            },
            {
                "cross_properties": ['music.site.com', 'artist', 'text/html', 0, 404, True, True],
                "counters": {
                    "metadata_nb": {
                        "title": 20,
                        "unique": 20
                    }
                }
            },
        ]

        c = MetricsConsolidator([stats_part_0, stats_part_1, stats_part_2, metadata_only_part])
        aggregated_data = c.consolidate(return_flatten=False)

        expected_data = {
            ('music.site.com', 'artist', 'text/html', 0, 200, True, True): {
                'pages_nb': 130,
                'metadata_nb': {
                    'h1': {
                        'filled': 100,
                        'unique': 90
                    }
                }
            },
            ('music.site.com', 'artist', 'text/html', 0, 404, True, True): {
                'pages_nb': 30,
                "metadata_nb": {
                    "title": 20,
                    "unique": 20
                }
            },
            ('www.site.com', 'homepage', 'text/html', 0, 200, True, True): {
                'pages_nb': 10,
            },
            ('my.site.com', 'product', 'text/html', 0, 200, True, True): {
                'pages_nb': 22,
                "inlinks_internal_nb": {
                    "total": 12,
                    "nofollow": 12,
                    "follow": 0,
                    "nofollow_combinations": {
                        "link_meta": 2,
                        "link": 10
                    }
                }
            },
            ('my.site.com', 'product', 'text/html', 0, 301, True, True): {
                'pages_nb': 30,
            }
        }

        for key, value in expected_data.iteritems():
            logger.info('Valid {}'.format(key))
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
            [1, 0, 'text/html', 0, 1, 200, 1200, 303, 456, True],
            [2, 0, 'text/html', 1, 1, 200, 1200, 303, 456, True],
            [3, 0, 'text/html', 1, 1, 200, 1200, 303, 456, True],
        ))

        a = MetadataAggregator(stream_patterns, stream_properties, stream_contents, stream_infos)

        expected_results = {
            ('www.site.com', 'product'):
            {
                "metadata_nb": {
                    "h1": {
                        "filled": 2,
                        "unique": 1
                    },
                    "h2": {
                        "filled": 1,
                        "unique": 0
                    },
                    "title": {
                        "filled": 0,
                        "unique": 0
                    },
                    "description": {
                        "filled": 0,
                        "unique": 0
                    },
                    "not_enough": 2
                }
            },
            ('www.site.com', 'homepage'):
            {
                "metadata_nb": {
                    "h1": {
                        "filled": 1,
                        "unique": 0
                    },
                    "h2": {
                        "filled": 1,
                        "unique": 0
                    },
                    "title": {
                        "filled": 1,
                        "unique": 1
                    },
                    "description": {
                        "filled": 0,
                        "unique": 0
                    },
                    "not_enough": 1
                }
            }
        }
        results = a.get()
        cross_properties = [k['cross_properties'] for k in results]
        homepage_idx = cross_properties.index(('www.site.com', 'homepage', 'text/html', 0, 200, True, True))
        product_idx = cross_properties.index(('www.site.com', 'product', 'text/html', 1, 200, True, True))

        self.assertEquals(results[product_idx]["counters"], flatten_dict(expected_results[('www.site.com', 'product')]))
        self.assertEquals(results[homepage_idx]["counters"], flatten_dict(expected_results[('www.site.com', 'homepage')]))

    def test_not_enough_metadata_bad_code(self):
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
            [1, 0, 'text/html', 0, 1, 200, 1200, 303, 456, True], # 200 code
            [2, 0, 'text/html', 1, 1, 200, 1200, 303, 456, True], # 200 code
            [3, 0, 'text/html', 1, 1, 301, 1200, 303, 456, True], # 301 code
        ))

        a = MetadataAggregator(stream_patterns, stream_properties, stream_contents, stream_infos)
        results = a.get()
        cross_properties = [k['cross_properties'] for k in results]
        product_idx = cross_properties.index(('www.site.com', 'product', 'text/html', 1, 200, True, True))

        self.assertEquals(results[product_idx]['counters']['metadata_nb.not_enough'], 1)


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
            [1, 0, 'text/html', 0, 1, 200, 1200, 303, 456, True],
            [2, 0, 'text/html', 1, 1, 200, 1200, 303, 456, True],
            [3, 0, 'text/html', 1, 1, 200, 1200, 303, 456, True],
        ))

        a = MetadataAggregator(stream_patterns, stream_properties, stream_contents, stream_infos)
        results = a.get()
        cross_properties = [k['cross_properties'] for k in results]
        homepage_idx = cross_properties.index(('www.site.com', 'homepage', 'text/html', 0, 200, True, True))
        product_idx = cross_properties.index(('www.site.com', 'product', 'text/html', 1, 200, True, True))
        self.assertEquals(results[homepage_idx]['counters']['metadata_nb.not_enough'], 0)
        self.assertEquals(results[product_idx]['counters']['metadata_nb.not_enough'], 1)
