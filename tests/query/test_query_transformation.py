import unittest

from cdf.metadata.url.es_backend_utils import ElasticSearchBackend
from cdf.query.query_parsing import QueryParser
from cdf.exceptions import BotifyQueryException
from cdf.utils.features import get_urls_data_format_definition

CRAWL_ID = 1


class QueryTransformationTestCase(unittest.TestCase):
    def setUp(self):
        self.es_backend = ElasticSearchBackend(get_urls_data_format_definition())
        self.parser = QueryParser(data_backend=self.es_backend)
        self.crawl_filter = {'term': {'crawl_id': CRAWL_ID}}
        self.not_crawled_filter = {'not': {'term': {'http_code': 0}}}

    def get_es_query(self, query, crawl_id):
        # no need to validate here
        return self.parser.get_es_query(query, crawl_id, validate=False)


class TestQueryTransformation(QueryTransformationTestCase):
    """Validation test for transformed ElasticSearch queries"""
    def test_process_filter(self):
        query_filters = {
            "filters": {
                "and": [
                    {"field": "http_code", "value": 200},
                    {"field": "delay2", "value": 100, "predicate": "gte"},
                ]
            }
        }

        default_filters = [
            {'field': 'crawl_id', 'value': 1},
            {'field': 'http_code', 'value': 0, 'predicate': 'gt'}
        ]

        query_filters = self.parser._merge_filters(query_filters, default_filters)
        target = query_filters['filters']['and']
        # assert on order
        # first filter should be that of the `crawl_id`
        self.assertEqual(target[0]['field'], 'crawl_id')

    def test_simple_filters(self):
        query = {
            "fields": ['id', 'url'],
            "filters": {"field": "http_code", "value": 200},
            "sort": ["id"]
        }

        expected_es_query = {
            'query': {
                'constant_score': {
                    'filter': {
                        'and': [
                            self.crawl_filter,
                            self.not_crawled_filter,
                            {'term': {'http_code': 200}}
                        ]
                    }
                }
            },
            'sort': [{'id': {'ignore_unmapped': True}}],
            '_source': ['id', 'url']
        }
        result = self.parser.get_es_query(query, CRAWL_ID)

        self.assertDictEqual(result, expected_es_query)

    def test_and_query(self):
        query = {
            "fields": ['id'],
            "filters": {
                "and": [
                    {"field": "http_code", "value": 200},
                    {"field": "delay_first_byte", "value": 100, "predicate": "gte"},
                ]
            },
            "sort": ["id"]
        }

        expected_es_query = {
            'sort': [{'id': {'ignore_unmapped': True}}],
            'query': {
                'constant_score': {
                    'filter': {
                        'and': [
                            self.crawl_filter,
                            self.not_crawled_filter,
                            {'term': {'http_code': 200}},
                            {'range': {'delay_first_byte': {'gte': 100}}},
                        ]
                    }
                }
            },
            '_source': ['id']
        }
        result = self.parser.get_es_query(query, CRAWL_ID)

        self.assertDictEqual(result, expected_es_query)

    def test_or_query(self):
        query = {
            "fields": ['id'],
            "filters": {
                "or": [
                    {"field": "http_code", "value": 200},
                    {"field": "http_code", "value": 301},
                ]
            },
            "sort": ["id"]
        }

        expected_es_query = {
            'sort': [{'id': {'ignore_unmapped': True}}],
            'query': {
                'constant_score': {
                    'filter': {
                        'and': [
                            self.crawl_filter,
                            self.not_crawled_filter,
                            {'or': [{'term': {'http_code': 200}},
                                    {'term': {'http_code': 301}}]}
                        ]
                    }
                }
            },
            '_source': ['id']
        }
        result = self.parser.get_es_query(query, CRAWL_ID)

        self.assertDictEqual(result, expected_es_query)

    def test_empty_query(self):
        query = {}

        expected_es_query = {
            'query': {
                'constant_score': {
                    'filter': {
                        'and': [self.crawl_filter, self.not_crawled_filter]
                    }
                }
            },
            'sort': [{'id': {'ignore_unmapped': True}}],
            '_source': ['url']
        }
        result = self.parser.get_es_query(query, CRAWL_ID)

        self.assertDictEqual(result, expected_es_query)

    def test_multi_fields_query(self):
        query = {
            'fields': ["metadata"],
            'filters': {
                'and': [
                    {'field': 'metadata.title.contents', 'predicate': 'any.starts', 'value': 'News'}
                ]
            }
        }

        expected_es_query = {
            '_source': ['metadata'],
            'sort': [{'id': {'ignore_unmapped': True}}],
            'query': {
                'constant_score': {
                    'filter': {
                        'and': [
                            self.crawl_filter, self.not_crawled_filter,
                            {'prefix': {'metadata.title.contents': 'News'}}
                        ]
                    }
                }
            }
        }
        result = self.parser.get_es_query(query, CRAWL_ID)
        self.assertDictEqual(result, expected_es_query)

    def test_exists_query(self):
        query = {
            'fields': ["metadata.h1"],
            'filters': {
                'and': [
                    {'field': 'metadata.title.contents', 'predicate': 'exists'}
                ]
            }
        }

        expected_es_query = {
            '_source': ['metadata.h1'],
            'query': {
                'constant_score': {
                    'filter': {
                        'and': [
                            self.crawl_filter,
                            self.not_crawled_filter,
                            {'or': [
                                {'exists': {'field': 'metadata.title.contents_exists'}},
                                {'exists': {'field': 'metadata.title.contents'}},
                            ]}
                        ]
                    }
                }
            },
            'sort': [{'id': {'ignore_unmapped': True}}]
        }
        result = self.parser.get_es_query(query, CRAWL_ID)
        self.assertDictEqual(result, expected_es_query)

    def test_sort(self):
        query = {
            'sort': ['url', {'id': {'order': 'desc'}}, 'metadata.h1.nb'],
            'fields': ['metadata.h1.contents']
        }

        expected_es_query = {
            '_source': ['metadata.h1.contents'],
            'query': {
                'constant_score': {
                    'filter': {
                        'and': [self.crawl_filter, self.not_crawled_filter]
                    }
                }
            },
            'sort': [
                {'url': {'ignore_unmapped': True}},
                {'id': {'order': 'desc', 'ignore_unmapped': True}},
                {'metadata.h1.nb': {'ignore_unmapped': True}}
            ]
        }
        result = self.parser.get_es_query(query, CRAWL_ID)
        self.assertDictEqual(result, expected_es_query)

    def test_between(self):
        query = {
            'fields': ['http_code'],
            'filters': {
                'field': 'http_code',
                'value': [123, 456],
                'predicate': 'between'
            }
        }

        expected_es_query = {
            '_source': ['http_code'],
            'query': {
                'constant_score': {
                    'filter': {
                        'and': [
                            self.crawl_filter,
                            self.not_crawled_filter,
                            {'range': {'http_code': {'gte': 123, 'lte': 456}}}
                        ]
                    }
                }
            },
            'sort': [{'id': {'ignore_unmapped': True}}]
        }
        result = self.parser.get_es_query(query, CRAWL_ID)
        self.assertDictEqual(result, expected_es_query)

    def test_bad_format_query(self):
        query = {
            'fields': ['http_code'],
            'filters': []
        }
        self.assertRaises(BotifyQueryException,
                          self.parser.get_es_query, query, CRAWL_ID)

        query = {
            'filters': {'and': [{}]}
        }
        self.assertRaises(BotifyQueryException,
                          self.parser.get_es_query, query, CRAWL_ID)


class TestAggregationTransformation(QueryTransformationTestCase):
    def test_distinct_agg(self):
        query = {
            'aggs': {
                'my_agg': {
                    'group_by': [{
                        'distinct': {
                            'field': 'http_code',
                            'size': 5
                        }
                    }],
                    'metric': 'count'
                }
            }
        }

        expected_agg = {
            'my_agg': {
                'terms': {
                    'field': 'http_code',
                    'size': 5,
                    'order': {'_term': 'asc'}
                },
                'aggs': {
                    'metricagg_00': {
                        'value_count': {
                            'field': 'id'
                        }
                    }
                }
            }
        }

        result = self.get_es_query(query, CRAWL_ID)
        self.assertEqual(expected_agg, result['aggs'])

    def test_distinct_agg_alias(self):
        query = {
            'aggs': {
                'my_agg': {
                    'group_by': ['http_code'],
                    'metric': 'count'
                }
            }
        }

        expected_agg = {
            'my_agg': {
                'terms': {
                    'field': 'http_code',
                    'size': 50,
                    'order': {'_term': 'asc'}
                },
                'aggs': {
                    'metricagg_00': {
                        'value_count': {
                            'field': 'id'
                        }
                    }
                }
            }
        }

        result = self.get_es_query(query, CRAWL_ID)
        self.assertEqual(expected_agg, result['aggs'])

    def test_range_agg(self):
        ranges = [{'from': 30, 'to': 40},
                  {'from': 50}]
        query = {
            'aggs': {
                'my_agg': {
                    'group_by': [{
                        'range': {
                            'field': 'http_code',
                            'ranges': ranges
                        }
                    }],
                }
            }
        }

        expected_agg = {
            'my_agg': {
                'range': {
                    'field': 'http_code',
                    'ranges': ranges
                },
                'aggs': {
                    'metricagg_00': {
                        'value_count': {
                            'field': 'id'
                        }
                    }
                }
            }
        }

        result = self.get_es_query(query, CRAWL_ID)
        self.assertEqual(expected_agg, result['aggs'])

    def test_multiple_aggs(self):
        query = {
            'aggs': {
                'my_agg_1': {
                    'group_by': [{'distinct': {'field': 'field1'}}],
                    'metrics': ['count']
                },
                'my_agg_2': {
                    'group_by': [{'distinct': {'field': 'field2'}}],
                    'metrics': ['count']
                }
            }
        }

        expected_agg = {
            'my_agg_1': {'terms': {'field': 'field1', 'size': 50, 'order': {'_term': 'asc'}}, 'aggs': {'metricagg_00': {'value_count': {'field': 'id'}}}},
            'my_agg_2': {'terms': {'field': 'field2', 'size': 50, 'order': {'_term': 'asc'}}, 'aggs': {'metricagg_00': {'value_count': {'field': 'id'}}}}
        }

        result = self.get_es_query(query, CRAWL_ID)
        self.assertEqual(expected_agg, result['aggs'])

    def test_agg_without_group(self):
        query = {
            'aggs': {
                'my_agg': {
                    'metrics': [{'sum': 'depth'}, 'count'],
                }
            }
        }

        expected_agg = {
            'metricagg_00_my_agg': {
                'sum': {
                    'field': 'depth'
                }
            },
            'metricagg_01_my_agg': {
                'value_count': {
                    'field': 'id'
                }
            }
        }

        result = self.get_es_query(query, CRAWL_ID)
        self.assertEqual(expected_agg, result['aggs'])
