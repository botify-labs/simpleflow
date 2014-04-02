"""Integration test for url query with ElasticSearch backend
"""

import unittest
import time
import copy

from nose.plugins.attrib import attr
from elasticsearch import Elasticsearch
from cdf.metadata.url import ELASTICSEARCH_BACKEND

from cdf.query.query import Query

ELASTICSEARCH_LOCATION = 'http://localhost:9200'
ELASTICSEARCH_INDEX = 'cdf_test'
CRAWL_ID = 1
DOC_TYPE = 'crawls'
REVISION_ID = 1

ES = Elasticsearch()
ES_BACKEND = ELASTICSEARCH_BACKEND

QUERY_ARGS = {
    'es_location': ELASTICSEARCH_LOCATION,
    'es_index': ELASTICSEARCH_INDEX,
    'es_doc_type': DOC_TYPE,
    'crawl_id': CRAWL_ID,
    'revision_number': REVISION_ID,
    'backend': ES_BACKEND
}


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


def _get_query_result(botify_query):
    return list(Query(botify_query=botify_query, **QUERY_ARGS).results)


URLS_FIXTURE = [
    {
        'id': 1,
        '_id': '%d:%d' % (CRAWL_ID, 1),
        'crawl_id': CRAWL_ID,
        'url': u'http://www.mysite.com/france/football/abcde/main.html',
        'path': u'/france/football/abcde/main.html',
        'http_code': 200,
        'metadata': {
            'title': {
                'nb': 1,
                'contents': ['My title'],
            },
            'h1': {
                'nb': 1,
                'contents': ['Welcome to our website']
            },
            'description': {'nb': 0},
            'h2': {
                'nb': 3,
                'contents': ['abcd', 'abc', 'botify']
            },
        },
        'outlinks_internal': {
            'urls': [
                [2, 0], # follow
                [3, 7], # link, meta, robots
                [5, 3], # link, meta / link to not crawled
            ],
            'urls_exists': True,
            'nb': {
                'total': 102,
                'unique': 2,
                'follow': {
                    'total': 101,
                    'unique': 1
                },
                'nofollow': {
                    'total': 0,
                }
            }
        },
        'canonical': {
            'to': {'url': {'url_id': 3}, 'url_exists': True},
            'from': {'urls': [2, 3, 4], 'nb': 3, 'urls_exists': True}
        },
    },
    {
        'id': 2,
        '_id': '%d:%d' % (CRAWL_ID, 2),
        'crawl_id': CRAWL_ID,
        'url': u'http://www.mysite.com/football/france/abc/abcde',
        'path': u'/football/france/abc/abcde',
        'http_code': 301,
        'metadata': {
            'h2': {
                'nb': 3,
                'contents': ['cba', 'foobar', 'botifyy']
            },
        },
        'inlinks_internal': {
            'urls': [
                [1, 0], # follow
                [3, 8], # follow
                [4, 5], # link, robots
            ],
            'urls_exists': True,
            'nb': {
                'total': 105,
                'unique': 3,
                'follow': {
                    'total': 104,
                    'unique': 2,
                },
                'nofollow': {
                    'total': 0
                }
            }
        },
        'canonical': {'to': {'url': {'url_id': 5}}, 'url_exists': True}
    },
    {
        'id': 3,
        '_id': '%d:%d' % (CRAWL_ID, 3),
        'crawl_id': CRAWL_ID,
        'url': u'http://www.mysite.com/football/article-s.html',
        'path': u'/football/article-s.html',
        'http_code': 200,

        'redirect': {
            'from': {
                'nb': 2,
                'urls': [
                    [1, 301],
                    [2, 301],
                ],
                'urls_exists': True
            }
        },
        'outlinks_errors': {
            '4xx': {
                'nb': 1,
                'urls': [4],
                'urls_exists': True,
            },
            'total': 1,
        },
    },
    {
        'id': 4,
        '_id': '%d:%d' % (CRAWL_ID, 4),
        'crawl_id': CRAWL_ID,
        'url': u'http://www.mysite.com/errors',
        'http_code': 200,
        'metadata': {
            'title': {
                'duplicates': {
                    'nb': 1,
                    'urls': [1],
                    'urls_exists': True
                }
            },
            'h1': {
                'duplicates': {
                    'nb': 2,
                    'urls': [2, 3],
                    'urls_exists': True
                }
            },
            'description': {
                'duplicates': {
                    'nb': 1,
                    'urls': [4],
                    'urls_exists': True
                }
            }
        },
        'outlinks_errors': {
            '3xx': {
                'nb': 3,
                'urls': [1, 2, 3],
                'urls_exists': True
            },
            '5xx': {
                'nb': 2,
                'urls': [2, 3],
                'urls_exists': True
            },
            'total': 5
        },
        'redirect': {
            'from': {
                'nb': 2,
                'urls': [
                    [1, 301],
                    [2, 301],
                ],
                'urls_exists': True
            }
        },
    },
    {
        'id': 5,
        '_id': '%d:%d' % (CRAWL_ID, 5),
        'crawl_id': CRAWL_ID,
        'url': u'http://www.notcrawled.com',
        'http_code': 0
    },
    {
        'id': 6,
        '_id': '%d:%d' % (CRAWL_ID, 6),
        'crawl_id': CRAWL_ID,
        'http_code': 200
    },
    {
        'id': 7,
        '_id': '%d:%d' % (CRAWL_ID, 7),
        'crawl_id': CRAWL_ID,
        'http_code': 301,
        'redirect': {
            'to': {'url': {'url_id': 5, 'http_code': 301},
                   'url_exists': True}
        },
    },
    {
        'id': 8,
        '_id': '%d:%d' % (CRAWL_ID, 8),
        'crawl_id': CRAWL_ID,
        'url': u'http://www.error.com',
        'http_code': -160
    }
]


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
                            {
                                'url': {
                                    'url': self.urls[1],
                                    'crawled': True
                                },
                                'http_code': 301
                            },
                            {
                                'url': {
                                    'url': self.urls[2],
                                    'crawled': True
                                },
                                'http_code': 301
                            }
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
                            {
                                'url': {
                                    'url': self.urls[1],
                                    'crawled': True
                                },
                                'http_code': 301
                            },
                            {
                                'url': {
                                    'url': self.urls[2],
                                    'crawled': True
                                },
                                'http_code': 301
                            }
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
                                        'crawled': False,
                                        'http_code': 301}}}
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
        self.assertEqual(result, [expected])

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
