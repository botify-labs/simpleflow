# -*- coding:utf-8 -*-
import unittest
import logging

from cdf.log import logger
from cdf.collections.tagging_stats.aggregator import (MetadataAggregator,
                                                      MetricsConsolidator)
from cdf.utils.dict import flatten_dict
logger.setLevel(logging.DEBUG)


def reverse_outlinks(stream_outlinks):
    stream_inlinks = []
    for k in stream_outlinks:
        stream_inlinks.append([k[3], k[1], k[2], k[0]])
    return filter(lambda i: i[0] > 0, sorted(stream_inlinks, key=lambda i: i[0]))


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
                        "unique": 1,
                        "not_filled": 0
                    },
                    "h2": {
                        "filled": 1,
                        "unique": 0,
                        "not_filled": 1,
                    },
                    "title": {
                        "filled": 0,
                        "unique": 0,
                        "not_filled": 2
                    },
                    "description": {
                        "filled": 0,
                        "unique": 0,
                        "not_filled": 2
                    },
                    "not_enough": 2
                }
            },
            ('www.site.com', 'homepage'):
            {
                "metadata_nb": {
                    "h1": {
                        "filled": 1,
                        "unique": 0,
                        "not_filled": 0
                    },
                    "h2": {
                        "filled": 1,
                        "unique": 0,
                        "not_filled": 0
                    },
                    "title": {
                        "filled": 1,
                        "unique": 1,
                        "not_filled": 0
                    },
                    "description": {
                        "filled": 0,
                        "unique": 0,
                        "not_filled": 1
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
