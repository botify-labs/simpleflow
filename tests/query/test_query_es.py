"""Integration test for url query with ElasticSearch backend
"""

import unittest
import time
import copy

from nose.plugins.attrib import attr
from elasticsearch import Elasticsearch
from cdf.metadata.url.backend import ElasticSearchBackend
from cdf.query.query import QueryBuilder
from cdf.testing.fixtures.dataformat import DATA_FORMAT_FIXTURE
from cdf.testing.fixtures.url_document import (
    URLS_FIXTURE, ELASTICSEARCH_LOCATION,
    ELASTICSEARCH_INDEX, CRAWL_ID, DOC_TYPE
)


def _get_simple_bql_query(field, predicate, value, fields=['id']):
    """Use below template to generate a simple bql query for tests"""
    return {
        'fields': fields,
        'filters': {
            'field': '{}'.format(field),
            'predicate': '{}'.format(predicate),
            'value': '{}'.format(value) if value is not None else None
        },
        'sort': ['id']
    }


def convert(input):
    if isinstance(input, dict):
        return {convert(key): convert(value) for key, value in input.iteritems()}
    elif isinstance(input, list):
        return [convert(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input


def _get_query_result(botify_query):
    q = QUERY_BUILDER.get_query(botify_query)
    return convert(list(q.results))


def _get_query_agg_result(botify_query):
    q = QUERY_BUILDER.get_query(botify_query)
    return q.aggs


ES = Elasticsearch()
ES_BACKEND = ElasticSearchBackend(DATA_FORMAT_FIXTURE)

QUERY_BUILDER = QueryBuilder(
    es_location=ELASTICSEARCH_LOCATION,
    es_index=ELASTICSEARCH_INDEX,
    es_doc_type=DOC_TYPE,
    crawl_id=CRAWL_ID,
    data_backend=ES_BACKEND
)


@attr(tag='elasticsearch')
class TestQueryES(unittest.TestCase):
    """Query tests that needs an ElasticSearch server process"""

    @classmethod
    def setUpClass(cls):
        try:
            # Try to delete test index if it exists
            ES.indices.delete(ELASTICSEARCH_INDEX)
        except:
            pass

        # Create index and put cdf's mapping
        mapping = {DOC_TYPE: copy.deepcopy(ES_BACKEND.mapping())['urls']}
        assert mapping[DOC_TYPE] == ES_BACKEND.mapping()['urls']
        ES.indices.create(index=ELASTICSEARCH_INDEX)
        ES.indices.put_mapping(index=ELASTICSEARCH_INDEX,
                               doc_type=DOC_TYPE, body=mapping)

        # Load test fixtures
        for url in URLS_FIXTURE:
            ES.index(ELASTICSEARCH_INDEX, DOC_TYPE, url, url['_id'])

        ES.indices.refresh(ELASTICSEARCH_INDEX)

        # Wait that all fixtures are indexed
        while ES.count(index=ELASTICSEARCH_INDEX)['count'] < len(URLS_FIXTURE):
            time.sleep(0.1)

    @classmethod
    def tearDownClass(cls):
        ES.indices.delete(ELASTICSEARCH_INDEX)
        pass

    def setUp(self):
        ES.indices.refresh(ELASTICSEARCH_INDEX)

        self.maxDiff = None

        self.urls = {item['id']: item.get('url', '') for item in URLS_FIXTURE}

        self.error_3xx = {
            'nb': 3,
            'urls': [self.urls[1], self.urls[2], self.urls[3]]
        }
        self.error_4xx = {
            'nb': 1,
            'urls': [self.urls[4]]
        }
        self.error_5xx = {
            'nb': 2,
            'urls': [self.urls[2], self.urls[3]]
        }
        self.any_missing = {'nb': 0}
        self.error_missing = {'nb': 0, 'urls': []}
        self.error_all_missing = {
            '3xx': self.error_missing,
            '4xx': self.error_missing,
            '5xx': self.error_missing,
            'total': 0
        }

    def tearDown(self):
        pass

    def test_starts_query(self):
        botify_query = _get_simple_bql_query('path', 'starts', '/france')
        results = _get_query_result(botify_query)
        self.assertEqual(results, [{'id': 1}])

        # `starts` predicate is strict, applied on `not_anlayzed`
        botify_query = _get_simple_bql_query('path', 'starts', 'france')
        results = _get_query_result(botify_query)
        self.assertEqual(results, [])

        botify_query = _get_simple_bql_query('path', 'starts', '/football')
        results = _get_query_result(botify_query)
        self.assertEqual(results, [{'id': 2}, {'id': 3}])

    def test_contains_query(self):
        botify_query = _get_simple_bql_query('url', 'contains', '/france/football')
        results = _get_query_result(botify_query)
        self.assertEqual(results, [{'id': 1}])

        # this query should also succeed
        botify_query = _get_simple_bql_query('url', 'contains', 'france/footb')
        results = _get_query_result(botify_query)
        self.assertEqual(results, [{'id': 1}])

    def test_ends_query(self):
        botify_query = _get_simple_bql_query('url', 'ends', 'article-s.html')
        results = _get_query_result(botify_query)
        self.assertEqual(results, [{'id': 3}])

        botify_query = _get_simple_bql_query('url', 'ends', 'main')
        results = _get_query_result(botify_query)
        self.assertEqual(results, [])

    def test_error_link_query(self):
        botify_query = _get_simple_bql_query('outlinks_errors.3xx.nb', 'gt', 0,
                                             fields=['outlinks_errors.3xx'])
        results = _get_query_result(botify_query)
        expected = {
            'outlinks_errors': {
                '3xx': self.error_3xx
            }
        }
        self.assertEqual(results, [expected])

        # search for `outlinks_errors` field should return all error links
        botify_query = _get_simple_bql_query('outlinks_errors.3xx.nb', 'gt', 0,
                                             fields=['outlinks_errors'])
        results = _get_query_result(botify_query)
        expected = {
            'outlinks_errors': {
                '3xx': self.error_3xx,
                '5xx': self.error_5xx,
                '4xx': self.error_missing,
                'total': 5
            }
        }
        self.assertEqual(results, [expected])

        # search only for `nb`
        botify_query = _get_simple_bql_query('outlinks_errors.3xx.nb', 'gt', 0,
                                             fields=['outlinks_errors.3xx.nb'])
        results = _get_query_result(botify_query)
        expected = {
            'outlinks_errors': {
                '3xx': {
                    'nb': 3
                }
            }
        }
        self.assertEqual(results, [expected])

        botify_query = _get_simple_bql_query('outlinks_errors.3xx.nb', 'gt', 0,
                                             fields=['outlinks_errors.3xx.urls'])
        results = _get_query_result(botify_query)
        expected = {
            'outlinks_errors': {
                '3xx': {'urls': self.error_3xx['urls']}
            }
        }
        self.assertEqual(results, [expected])

    def test_query_missing_fields(self):
        """`error_links` is missing in the first 2 docs"""
        # retrieve parent field
        botify_query = _get_simple_bql_query('http_code', 'gt', 0,
                                             fields=['id', 'outlinks_errors'])
        results = _get_query_result(botify_query)
        expected = [
            {'id': 1, 'outlinks_errors': self.error_all_missing},
            {'id': 2, 'outlinks_errors': self.error_all_missing},
            {
                'id': 3,
                'outlinks_errors': {
                    '4xx': self.error_4xx,
                    '3xx': self.error_missing,
                    '5xx': self.error_missing,
                    'total': 1
                }
            },
            {
                'id': 4,
                'outlinks_errors': {
                    '3xx': self.error_3xx,
                    '4xx': self.error_missing,
                    '5xx': self.error_5xx,
                    'total': 5
                }
            },
            {'id': 6, 'outlinks_errors': self.error_all_missing},
            {'id': 7, 'outlinks_errors': self.error_all_missing}
        ]

        self.assertEqual(results, expected)

    def test_inlinks_query(self):
        botify_query = _get_simple_bql_query('inlinks_internal.nb.total', 'gt', 0,
                                             fields=['id', 'inlinks_internal.urls'])
        result = _get_query_result(botify_query)
        expected = {
            'id': 2,
            'inlinks_internal': {
                'urls': [
                    {
                        'url': {
                            'url': self.urls[1],
                            'crawled': True
                        },
                        'status': ['follow'],
                    },
                    {
                        'url': {
                            'url': self.urls[3],
                            'crawled': True
                        },
                        'status': ['follow'],
                    },
                    {
                        'url': {
                            'url': self.urls[4],
                            'crawled': True
                        },
                        'status': ['nofollow_robots', 'nofollow_link'],
                    }
                ]
            }
        }
        self.assertEqual(result, [expected])

    def test_outlinks_query(self):
        botify_query = _get_simple_bql_query('outlinks_internal.nb.total', 'gt', 0,
                                             fields=['id', 'outlinks_internal.urls'])
        result = _get_query_result(botify_query)
        expected = {
            'id': 1,
            'outlinks_internal': {
                'urls': [
                    {
                        'url': {
                            'url': self.urls[2],
                            'crawled': True
                        },
                        'status': ['follow'],
                    },
                    {
                        'url': {
                            'url': self.urls[3],
                            'crawled': True
                        },
                        'status': ['nofollow_robots', 'nofollow_meta', 'nofollow_link'],
                    },
                    {
                        'url': {
                            'url': self.urls[5],
                            'crawled': False
                        },
                        'status': ['nofollow_meta', 'nofollow_link'],
                    }
                ]
            }
        }
        self.assertEqual(result, [expected])

    def test_canonical_to_query(self):
        # external url
        botify_query = _get_simple_bql_query('id', 'eq', 1,
                                             fields=['canonical.to'])
        result = _get_query_result(botify_query)
        expected = {
            'canonical': {
                'to': {
                    'url': {
                        'url': self.urls[3],
                        # auto-gen as a default value
                        # will not impact front-end
                        'crawled': True,
                    },
                    'equal': False
                }
            },
        }
        self.assertEqual(result, [expected])

    def test_canonical_to_not_crawled(self):
        # internal url and it's not crawled
        botify_query = _get_simple_bql_query('id', 'eq', 2,
                                             fields=['canonical.to'])
        result = _get_query_result(botify_query)
        expected = {
            'canonical': {
                'to': {
                    'url': {
                        'url': 'http://www.notcrawled.com',
                        'crawled': False,
                    },
                    'equal': False
                }
            },
        }
        self.assertEqual(result, [expected])

    def test_canonical_from_query(self):
        botify_query = _get_simple_bql_query('canonical.from.nb', 'gt', 0,
                                             fields=['canonical.from'])
        result = _get_query_result(botify_query)
        expected = {
            'canonical': {
                'from': {
                    'urls': [self.urls[2], self.urls[3], self.urls[4]],
                    'nb': 3
                }
            }
        }
        self.assertEqual(result, [expected])

    def test_redirects_from_query(self):
        botify_query = _get_simple_bql_query('redirect.from.nb', 'gt', 0,
                                             fields=['id', 'redirect.from'])
        result = _get_query_result(botify_query)
        expected = [
            {
                'id': 3,
                'redirect': {
                    'from': {
                        'nb': 2,
                        'urls': [
                            [self.urls[1], 301],
                            [self.urls[2], 301],
                        ]
                    }
                }
            },
            {
                'id': 4,
                'redirect': {
                    'from': {
                        'nb': 2,
                        'urls': [
                            [self.urls[1], 301],
                            [self.urls[2], 301],
                        ]
                    }
                }
            }
        ]
        self.assertEqual(result, expected)

    def test_redirects_to_exists(self):
        botify_query = _get_simple_bql_query('redirect.to.url', 'exists', value=None,
                                             fields=['redirect.to'])
        result = _get_query_result(botify_query)
        expected = {
            'redirect': {'to': {'url': {'url': self.urls[5],
                                        'crawled': False}}}
        }
        self.assertEqual(result, [expected])

    def test_no_redirects_canonical(self):
        botify_query = _get_simple_bql_query('id', 'eq', 6,
                                             fields=['redirect.to'])
        result = _get_query_result(botify_query)
        self.assertEqual(result, [{'redirect': {'to': {'url': None}}}])

        botify_query = _get_simple_bql_query('id', 'eq', 6,
                                             fields=['canonical.to'])
        result = _get_query_result(botify_query)
        self.assertEqual(result, [{'canonical': {'to': {'url': None, 'equal': False}}}])

    def test_metadata_duplicate_query(self):
        botify_query = _get_simple_bql_query('metadata.title.duplicates.nb', 'gt', 0,
                                             fields=['metadata.title.duplicates',
                                                     'metadata.h1.duplicates',
                                                     'metadata.description.duplicates'])
        result = _get_query_result(botify_query)
        expected = {
            'metadata': {
                'title': {
                    'duplicates': {
                        'nb': 1,
                        'urls': [{'url': self.urls[1], 'crawled': True}],
                        'is_first': False
                    }
                },
                'h1': {
                    'duplicates': {
                        'nb': 2,
                        'urls': [
                            {'url': self.urls[2], 'crawled': True},
                            {'url': self.urls[3], 'crawled': True}
                        ],
                        'is_first': False
                    }
                },
                'description': {
                    'duplicates': {
                        'nb': 1,
                        'urls': [{'url': self.urls[4], 'crawled': True}],
                        'is_first': False
                    }
                }
            }
        }
        self.assertEqual(convert(result), [expected])

    def test_sort_query(self):
        botify_query = {
            'sort': [{'http_code': {'order': 'desc'}}, 'id'],
            'fields': ['id', 'http_code'],
        }
        result = _get_query_result(botify_query)
        expected = [
            {'id': 2, 'http_code': 301},
            {'id': 7, 'http_code': 301},
            {'id': 1, 'http_code': 200},
            {'id': 3, 'http_code': 200},
            {'id': 4, 'http_code': 200},
            {'id': 6, 'http_code': 200},
            {'id': 8, 'http_code': -160},
            # url 5 is ignored since it's not crawled
        ]
        self.assertEqual(result, expected)

    def test_between_query(self):
        botify_query = {
            'fields': ['id'],
            'filters': {
                'predicate': 'between',
                'field': 'id',
                'value': [0, 3]
            }
        }
        result = _get_query_result(botify_query)
        expected = [
            {'id': 1},
            {'id': 2},
            {'id': 3},
        ]
        self.assertEqual(result, expected)

    def test_any_starts(self):
        botify_query = _get_simple_bql_query('metadata.h2.contents', 'any.starts', 'ab')
        results = _get_query_result(botify_query)
        self.assertEqual(results, [{'id': 1}])

    def test_any_contains(self):
        botify_query = _get_simple_bql_query('metadata.h2.contents', 'any.contains', 'otif')
        results = _get_query_result(botify_query)
        self.assertEqual(results, [{'id': 1}, {'id': 2}])

    def test_any_equals(self):
        botify_query = _get_simple_bql_query('metadata.h2.contents', 'any.eq', 'botify')
        results = _get_query_result(botify_query)

        # it's exact match, url 2 doesn't qualify
        self.assertEqual(results, [{'id': 1}])

    def test_any_ends(self):
        botify_query = _get_simple_bql_query('metadata.h2.contents', 'any.ends', 'y')
        results = _get_query_result(botify_query)
        self.assertEqual(results, [{'id': 1}, {'id': 2}])

    def test_agg_distinct(self):
        botify_query = {
            'aggs': [
                {
                    'group_by': ['http_code']
                }
            ]
        }
        results = _get_query_agg_result(botify_query)
        # pages with `0` http_code are filtered out by query
        expected = [
            {
                'groups': [{'metrics': [1], 'key': [-160]},
                           {'metrics': [4], 'key': [200]},
                           {'metrics': [2], 'key': [301]}]
            }
        ]
        self.assertEqual(results, expected)

    def test_agg_range(self):
        botify_query = {
            'aggs': [
                {
                    'group_by': [
                        {'range': {
                            'field': 'http_code',
                            'ranges': [
                                {'to': '1'},
                                {'from': '1', 'to': '201'},
                                {'from': '201'}
                            ]
                        }}
                    ]
                }
            ]
        }
        results = _get_query_agg_result(botify_query)
        expected = [
            {
                'groups': [
                    {'key': [{'to': 1}], 'metrics': [1]},
                    {'key': [{'to': 201, 'from': 1}], 'metrics': [4]},
                    {'key': [{'from': 201}], 'metrics': [2]},
                ]
            }
        ]
        self.assertEqual(results, expected)

    def test_agg_multiple_aggs(self):
        botify_query = {
            'aggs': [
                {'group_by': ['http_code']},
                {'group_by': ['http_code']}
            ]
        }
        results = _get_query_agg_result(botify_query)
        expected_groups = [{'metrics': [1], 'key': [-160]},
                           {'metrics': [4], 'key': [200]},
                           {'metrics': [2], 'key': [301]}]
        expected = [
            {'groups': expected_groups},
            {'groups': expected_groups}
        ]
        self.assertEqual(results, expected)

    def test_agg_nested(self):
        botify_query = {
            'aggs': [
                {
                    'group_by': ['http_code', 'depth']
                }
            ]
        }
        results = _get_query_agg_result(botify_query)
        expected = [
            {'key': [200, 1], 'metrics': [1]},
            {'key': [200, 2], 'metrics': [2]},
            {'key': [301, 2], 'metrics': [2]},
        ]
        self.assertItemsEqual(results[0]['groups'], expected)

    def test_agg_query_nested_multiple(self):
        botify_query = {
            'aggs': [
                {
                    'group_by': ['http_code', 'depth'],
                    'metrics': [
                        {"sum": "outlinks_internal.nb.total"},
                        "count"
                    ]
                }
            ]
        }
        results = _get_query_agg_result(botify_query)
        expected = [
            {'key': [200, 1], 'metrics': [102.0, 1]},
            {'key': [200, 2], 'metrics': [0.0, 2]},
            {'key': [301, 2], 'metrics': [0.0, 2]},
        ]
        self.assertItemsEqual(results[0]['groups'], expected)

    def test_agg_without_group(self):
        botify_query = {
            'aggs': [
                {
                    'metrics': [
                        {"sum": "outlinks_internal.nb.total"},
                        "count"
                    ]
                }
            ]
        }
        results = _get_query_agg_result(botify_query)
        expected = {'metrics': [102.0, 7]}
        self.assertEquals(results[0], expected)

    def test_min_aggregator(self):
        botify_query = {
            'aggs': [
                {
                    'metrics': [
                        {"min": "http_code"}
                    ]
                }
            ]
        }
        results = _get_query_agg_result(botify_query)
        expected = {'metrics': [-160.0]}
        self.assertEquals(results[0], expected)

    def test_max_aggregator(self):
        botify_query = {
            'aggs': [
                {
                    'metrics': [
                        {"max": "http_code"}
                    ]
                }
            ]
        }
        results = _get_query_agg_result(botify_query)
        expected = {'metrics': [301.0]}
        self.assertEquals(results[0], expected)

    def test_avg_aggregator(self):
        botify_query = {
            'aggs': [
                {
                    'metrics': [
                        {"avg": "depth"}
                    ]
                }
            ]
        }
        results = _get_query_agg_result(botify_query)
        expected = {'metrics': [1.8]}
        self.assertEquals(results[0], expected)

    def test_mixed_aggregation(self):
        botify_query = {
            "filters": {
                "and": [
                    {
                        "field": "http_code",
                        "value": [
                            200,
                            299
                        ],
                        "predicate": "between"
                    }
                ]
            },
            "aggs": [
                # first aggregation,
                # have name, default metric: `count` documents
                {
                    "name": "depth_http_code",
                    "group_by": ["http_code", "depth"],
                },
                # second aggregation
                # have name, have `group_by`
                {
                    "name": "title",
                    "group_by": [
                        {
                            "range": {
                                "field": "metadata.title.nb",
                                "ranges": [
                                    {
                                        "from": 0,
                                        "to": 1
                                    },
                                    {
                                        "from": 1
                                    }
                                ]
                            }
                        }
                    ]
                },
                # third aggregation
                # no name, no `group_by`, multiple metrics
                {
                    "metrics": [
                        {
                            "avg": "depth"
                        },
                        "count"
                    ]
                },
            ]
        }
        result = _get_query_agg_result(botify_query)
        expected = [
            {'groups': [{'key': [200, 1], 'metrics': [1]},
                        {'key': [200, 2], 'metrics': [2]}]},
            {'groups': [{'key': [{'to': 1, 'from': 0}], 'metrics': [0]},
                        {'key': [{'from': 1}], 'metrics': [1]}]},
            {'metrics': [1.6666666666666667, 4]}
        ]
        self.assertEqual(result, expected)
