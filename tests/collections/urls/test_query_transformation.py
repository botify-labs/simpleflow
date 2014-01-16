import unittest

from nose.plugins.attrib import attr
from elasticsearch import Elasticsearch

from cdf.collections.urls.query_transformer import get_es_query, _merge_filters
from cdf.constants import URLS_DATA_MAPPING

CRAWL_ID = 1
ELASTICSEARCH_LOCATION = 'http://localhost:9200'
ELASTICSEARCH_INDEX = 'cdf_query_test'
DOC_TYPE = 'crawl_%d' % CRAWL_ID
REVISION_ID = 1

ES = Elasticsearch()


@attr(tag='elasticsearch')
class TestQueryTransformation(unittest.TestCase):
    """Validation test for transformed ElasticSearch queries,
    using ElasticSearch query validate API"""

    @classmethod
    def setUpClass(cls):
        try:
            # Try to delete test index if it exists
            ES.indices.delete(ELASTICSEARCH_INDEX)
        except:
            pass

        # Create index and put cdf's mapping
        ES.indices.create(ELASTICSEARCH_INDEX)
        ES.indices.put_mapping(ELASTICSEARCH_INDEX,
                               DOC_TYPE,
                               URLS_DATA_MAPPING)

    @classmethod
    def tearDownClass(cls):
        ES.indices.delete(ELASTICSEARCH_INDEX)

    def setUp(self):
        self.crawl_filter = {'term': {'crawl_id': CRAWL_ID}}
        self.not_crawled_filter = {'not': {'term': {'http_code': 0}}}

    @staticmethod
    def is_valid_es_query(es_query):
        # Every result ES query should have these 3 components
        if ('fields' not in es_query or 'sort' not in es_query or
                    'query' not in es_query):
            return False

        response = ES.indices.validate_query(ELASTICSEARCH_INDEX,
                                             DOC_TYPE, es_query, explain=True)
        return response['valid']

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

        query_filters = _merge_filters(query_filters, default_filters)
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
            'fields': ['id', 'url']
        }
        result = get_es_query(query, CRAWL_ID)

        self.assertDictEqual(result, expected_es_query)

    def test_and_query(self):
        query = {
            "fields": ['id'],
            "filters": {
                "and": [
                    {"field": "http_code", "value": 200},
                    {"field": "delay2", "value": 100, "predicate": "gte"},
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
                            {'range': {'delay2': {'from': 100}}},
                        ]
                    }
                }
            },
            'fields': ['id']
        }
        result = get_es_query(query, CRAWL_ID)

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
            'fields': ['id']
        }
        result = get_es_query(query, CRAWL_ID)

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
            'fields': ['url']
        }
        result = get_es_query(query, CRAWL_ID)

        self.assertDictEqual(result, expected_es_query)

    def test_multi_fields_query(self):
        query = {
            'fields': ["metadata"],
            'filters': {
                'and': [
                    {'field': 'metadata.title', 'predicate': 'starts', 'value': 'News'}
                ]
            }
        }

        expected_es_query = {
            'fields': ['metadata'],
            'sort': [{'id': {'ignore_unmapped': True}}],
            'query': {
                'constant_score': {
                    'filter': {
                        'and': [
                            {'and': [{'range': {'http_code': {'from': 0, 'include_lower': False}}},
                                     {'term': {'crawl_id': 1}}]},
                            {'prefix': {'metadata.title.untouched': 'News'}}
                        ]
                    }
                }
            }
        }
        self.assertItemsEqual(get_es_query(query, CRAWL_ID), expected_es_query)
        # self.is_valid_es_query(expected_es_query)

    def test_not_null_query(self):
        query = {
            'fields': ["metadata.h1"],
            'filters': {
                'and': [
                    {'field': 'metadata.title', 'predicate': 'not_null'}
                ]
            }
        }

        expected_es_query = {
            'fields': ['metadata.h1'],
            'query': {
                'constant_score': {
                    'filter': {
                        'and': [
                            self.crawl_filter,
                            self.not_crawled_filter,
                            {'exists': {'field': 'metadata.title'}}
                        ]
                    }
                }
            },
            'sort': [{'id': {'ignore_unmapped': True}}]
        }
        result = get_es_query(query, CRAWL_ID)
        self.assertDictEqual(result, expected_es_query)

    def test_sort(self):
        query = {
            'sort': ['url', {'id': {'order': 'desc'}}, 'metadata.h1'],
            'fields': ['metadata.h1']
        }

        expected_es_query = {
            'fields': ['metadata.h1'],
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
                {'metadata.h1': {'ignore_unmapped': True}}
            ]
        }
        result = get_es_query(query, CRAWL_ID)
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
            'fields': ['http_code'],
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
        result = get_es_query(query, CRAWL_ID)
        self.assertDictEqual(result, expected_es_query)