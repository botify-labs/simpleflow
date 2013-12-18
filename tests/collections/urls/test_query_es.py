import unittest

from nose.plugins.attrib import attr
from elasticsearch import Elasticsearch
import time

from cdf.collections.urls.query import Query
from cdf.constants import URLS_DATA_MAPPING

ELASTICSEARCH_LOCATION = 'http://localhost:9200'
ELASTICSEARCH_INDEX = 'cdf_test'
CRAWL_ID = 1
CRAWL_NAME = 'crawl_%d' % CRAWL_ID
REVISION_ID = 1

ES = Elasticsearch()

QUERY_ARGS = (ELASTICSEARCH_LOCATION, ELASTICSEARCH_INDEX,
              'crawl_{}'.format(CRAWL_ID), CRAWL_ID, REVISION_ID)


def _get_simple_bql_query(field, predicate, value, fields=['id']):
    """Use below template to generate a simple bql query for tests"""
    return {
        'fields': fields,
        'filters': {
            'field': '{}'.format(field),
            'predicate': '{}'.format(predicate),
            'value': '{}'.format(value)
        },
        'sort': ['id']
    }


URLS_FIXTURE = [
    {
        'id': 1,
        '_id': '%d:%d' % (CRAWL_ID, 1),
        'crawl_id': CRAWL_ID,
        'url': u'http://www.mysite.com/france/football/abcde/main.html',
        'path': u'/france/football/abcde/main.html',
        'http_code': 200,
        'metadata': {
            'title': ['My title'],
            'h1': ['Welcome to our website']
        },
        'metadata_nb': {
            'title': 1,
            'h1': 1,
            'h2': 0,
            'description': 0
        },
        'outlinks_internal': [
            [2, 0, 100], # follow
            [3, 7, 1], # link, meta, robots
            [5, 3, 1], # link, meta / link to not crawled
        ],
        'outlinks_internal_nb': {
            'total_unique': 2,
            'nofollow': 0,
            'follow_unique': 1,
            'follow': 101,
            'total': 102
        },
        'canonical_to': {
            'url_id': 3
        },
        'canonical_from': [2, 3, 4],
        'canonical_from_nb': 3
    },
    {
        'id': 2,
        '_id': '%d:%d' % (CRAWL_ID, 2),
        'crawl_id': CRAWL_ID,
        'url': u'http://www.mysite.com/football/france/abc/abcde',
        'path': u'/football/france/abc/abcde',
        'http_code': 301,
        'inlinks_internal': [
            [1, 0, 100], # follow
            [3, 8, 4], # follow
            [4, 5, 1], # link, robots
        ],
        'inlinks_internal_nb': {
            'total_unique': 3,
            'nofollow': 0,
            'follow_unique': 2,
            'follow': 104,
            'total': 105
        },
        'canonical_to': {
            'url_id': 5
        }
    },
    {
        'id': 3,
        '_id': '%d:%d' % (CRAWL_ID, 3),
        'crawl_id': CRAWL_ID,
        'url': u'http://www.mysite.com/football/article-s.html',
        'path': u'/football/article-s.html',
        'http_code': 200,
        'redirects_from_nb': 2,
        'redirects_from': [
            {'http_code': 301, 'url_id': 1},
            {'http_code': 301, 'url_id': 2}
        ],
        'error_links': {
            '4xx': {
                'nb': 1,
                'urls': [4]
            }
        }
    },
    {
        'id': 4,
        '_id': '%d:%d' % (CRAWL_ID, 4),
        'crawl_id': CRAWL_ID,
        'url': u'http://www.mysite.com/errors',
        'http_code': 200,
        'error_links': {
            "3xx": {
                "nb": 3,
                "urls": [1, 2, 3]
            },
            "5xx": {
                "nb": 2,
                "urls": [2, 3]
            }
        },
        'metadata_duplicate': {
            'title': [1],
            'h1': [2, 3],
            'description': [4]
        },
        'metadata_duplicate_nb': {
            'title': 1,
            'h1': 2,
            'description': 1
        },
        'redirects_from_nb': 2,
        'redirects_from': [
            {'http_code': 301, 'url_id': 1},
            {'http_code': 301, 'url_id': 2}
        ],
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
        'redirects_to': {
            'http_code': 301,
            'url_id': 5
        }
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
        ES.indices.create(ELASTICSEARCH_INDEX)
        ES.indices.put_mapping(ELASTICSEARCH_INDEX,
                               'crawl_{}'.format(CRAWL_ID),
                               URLS_DATA_MAPPING)
        # Load test fixtures
        for url in URLS_FIXTURE:
            ES.index(ELASTICSEARCH_INDEX,
                     'crawl_{}'.format(CRAWL_ID), url, url['_id'])

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

    def tearDown(self):
        pass

    def test_starts_query(self):
        bql_query = _get_simple_bql_query('path', 'starts', '/france')
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        self.assertItemsEqual(results, [{'id': 1}])

        # `starts` predicate is strict, applied on `not_anlayzed`
        bql_query = _get_simple_bql_query('path', 'starts', 'france')
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        self.assertItemsEqual(results, [])

        bql_query = _get_simple_bql_query('path', 'starts', '/football')
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        self.assertItemsEqual(results, [{'id': 2}, {'id': 3}])

    def test_contains_query(self):
        bql_query = _get_simple_bql_query('url', 'contains', '/france/football')
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        self.assertItemsEqual(results, [{'id': 1}])

        # this query should also succeed
        bql_query = _get_simple_bql_query('url', 'contains', 'france/footb')
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        self.assertItemsEqual(results, [{'id': 1}])

    def test_ends_query(self):
        bql_query = _get_simple_bql_query('url', 'ends', 'article-s.html')
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        self.assertItemsEqual(results, [{'id': 3}])

        bql_query = _get_simple_bql_query('url', 'ends', 'main')
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        self.assertItemsEqual(results, [])

    def test_error_link_query(self):
        bql_query = _get_simple_bql_query('error_links.3xx.nb', 'gt', 0,
                                          fields=['error_links.3xx'])
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = {
            'error_links': {
                '3xx': self.error_3xx
            }
        }
        self.assertItemsEqual(results, [expected])

        # search for `error_links` field should return all error links
        bql_query = _get_simple_bql_query('error_links.3xx.nb', 'gt', 0,
                                          fields=['error_links'])
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = {
            'error_links': {
                '3xx': self.error_3xx,
                '5xx': self.error_5xx
            }
        }
        self.assertItemsEqual(results, [expected])

        # search only for `nb`
        bql_query = _get_simple_bql_query('error_links.3xx.nb', 'gt', 0,
                                          fields=['error_links.3xx.nb'])
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = {
            'error_links': {
                '3xx': {
                    'nb': 3
                }
            }
        }
        self.assertItemsEqual(results, [expected])

    def test_query_missing_fields(self):
        """`error_links` is missing in the first 2 docs"""
        # retrieve parent field
        bql_query = _get_simple_bql_query('http_code', 'gt', 0,
                                          fields=['id', 'error_links'])
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = [
            {'id': 1},
            {'id': 2},
            {
                'id': 3,
                'error_links': {
                    '4xx': self.error_4xx
                }
            },
            {
                'id': 4,
                'error_links': {
                    '3xx': self.error_3xx,
                    '5xx': self.error_5xx
                }
            },
            {'id': 6},
            {'id': 7}
        ]

        self.assertItemsEqual(results, expected)

    def test_inlinks_query(self):
        bql_query = _get_simple_bql_query('inlinks_internal_nb.total', 'gt', 0,
                                          fields=['id', 'inlinks_internal'])
        result = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = {
            'id': 2,
            'inlinks_internal': [
                {
                    'url': {
                        'url': self.urls[1],
                        'crawled': True
                    },
                    'status': ['follow'],
                    'nb_links': 100
                },
                {
                    'url': {
                        'url': self.urls[3],
                        'crawled': True
                    },
                    'status': ['follow'],
                    'nb_links': 4
                },
                {
                    'url': {
                        'url': self.urls[4],
                        'crawled': True
                    },
                    'status': ['nofollow_robots', 'nofollow_link'],
                    'nb_links': 1
                }
            ]
        }
        self.assertItemsEqual(result, [expected])

    def test_outlinks_query(self):
        bql_query = _get_simple_bql_query('outlinks_internal_nb.total', 'gt', 0,
                                          fields=['id', 'outlinks_internal'])
        result = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = {
            'id': 1,
            'outlinks_internal': [
                {
                    'url': {
                        'url': self.urls[2],
                        'crawled': True
                    },
                    'status': ['follow'],
                    'nb_links': 100
                },
                {
                    'url': {
                        'url': self.urls[3],
                        'crawled': True
                    },
                    'status': ['nofollow_robots', 'nofollow_meta', 'nofollow_link'],
                    'nb_links': 1
                },
                {
                    'url': {
                        'url': self.urls[5],
                        'crawled': False
                    },
                    'status': ['nofollow_meta', 'nofollow_link'],
                    'nb_links': 1
                }
            ]
        }
        self.assertItemsEqual(result, [expected])

    def test_canonical_to_query(self):
        # external url
        bql_query = _get_simple_bql_query('id', 'eq', 1,
                                          fields=['canonical_to'])
        result = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = {
            'canonical_to': {
                'url': self.urls[3],
                'crawled': True
            },
        }
        self.assertItemsEqual(result, [expected])

    def test_canonical_to_not_crawled(self):
        # internal url and it's not crawled
        bql_query = _get_simple_bql_query('id', 'eq', 2,
                                          fields=['canonical_to'])
        result = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = {
            'canonical_to': {'url': 'http://www.notcrawled.com', 'crawled': False},
        }
        self.assertItemsEqual(result, [expected])

    def test_canonical_from_query(self):
        bql_query = _get_simple_bql_query('canonical_from_nb', 'gt', 0,
                                          fields=['canonical_from'])
        result = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = {
            'canonical_from': [self.urls[2], self.urls[3], self.urls[4]]
        }
        self.assertItemsEqual(result, [expected])

    def test_redirects_from_query(self):
        bql_query = _get_simple_bql_query('redirects_from_nb', 'gt', 0,
                                          fields=['id', 'redirects_from'])
        result = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = [
            {
                'id': 3,
                'redirects_from': [
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
            },
            {
                'id': 4,
                'redirects_from': [
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
        ]
        self.assertItemsEqual(result, expected)

    def test_redirects_to_not_crawled(self):
        bql_query = _get_simple_bql_query('redirects_to', 'not_null', 0,
                                          fields=['redirects_to'])
        result = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = {
            'redirects_to': {'url': self.urls[5], 'crawled': False}
        }
        self.assertItemsEqual(result, [expected])

    def test_metadata_duplicate_query(self):
        bql_query = _get_simple_bql_query('metadata_duplicate_nb.title', 'gt', 0,
                                          fields=['metadata_duplicate'])
        result = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = {
            'metadata_duplicate': {
                'title': [
                    {'url': self.urls[1], 'crawled': True}
                ],
                'h1': [
                    {'url': self.urls[2], 'crawled': True},
                    {'url': self.urls[3], 'crawled': True}
                ],
                'description': [
                    {'url': self.urls[4], 'crawled': True}
                ]
            }
        }
        self.assertItemsEqual(result, [expected])

    def test_default_value(self):
        bql_query = _get_simple_bql_query('id', 'eq', 6,
                                          fields=['metadata_duplicate',
                                                  'metadata_nb'])
        result = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = {
            'metadata_duplicate': {
                'title': [], 'h1': [], 'description': []
            },
            'metadata_nb': {
                'title': 0, 'h1': 0, 'description': 0, 'h2': 0
            }
        }
        self.assertItemsEqual(result, [expected])

    def test_sort_query(self):
        bql_query = {
            'sort': [{'http_code': {'order': 'desc'}}, 'id'],
            'fields': ['id'],
        }
        result = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = [
            {'id': 2},
            {'id': 7},
            {'id': 1},
            {'id': 3},
            {'id': 4},
            {'id': 6}
            # url 5 is ignored since it's not crawled
        ]
        self.assertItemsEqual(result, expected)

    def test_between_query(self):
        bql_query = {
            'fields': ['id'],
            'filters': {
                'predicate': 'between',
                'field': 'id',
                'value': [0, 3]
            }
        }
        result = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = [
            {'id': 1},
            {'id': 2},
            {'id': 3},
        ]
        self.assertItemsEqual(result, expected)