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


def get_simple_bql_query(field, predicate, value, fields=['id']):
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


@attr(tag='elasticsearch')
class TestQueryES(unittest.TestCase):
    """Query tests that needs an ElasticSearch server process"""

    @classmethod
    def setUpClass(cls):
        urls = [
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
            },
            {
                'id': 2,
                '_id': '%d:%d' % (CRAWL_ID, 2),
                'crawl_id': CRAWL_ID,
                'url': u'http://www.mysite.com/football/france/abc/abcde',
                'path': u'/football/france/abc/abcde',
                'http_code': 301,
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
                ]
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
                }
            }
        ]

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
        for url in urls:
            ES.index(ELASTICSEARCH_INDEX,
                     'crawl_{}'.format(CRAWL_ID), url, url['_id'])

        # Wait that all fixtures are indexed
        while ES.count(index=ELASTICSEARCH_INDEX)['count'] < len(urls):
            time.sleep(0.1)

    @classmethod
    def tearDownClass(cls):
        ES.indices.delete(ELASTICSEARCH_INDEX)
        pass

    def setUp(self):
        ES.indices.refresh(ELASTICSEARCH_INDEX)

    def tearDown(self):
        pass

    def test_starts_query(self):
        bql_query = get_simple_bql_query('path', 'starts', '/france')
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        self.assertItemsEqual(results, [{'id': 1}])

        # `starts` predicate is strict, applied on `not_anlayzed`
        bql_query = get_simple_bql_query('path', 'starts', 'france')
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        self.assertItemsEqual(results, [])

        bql_query = get_simple_bql_query('path', 'starts', '/football')
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        self.assertItemsEqual(results, [{'id': 2}, {'id': 3}])

    def test_contains_query(self):
        bql_query = get_simple_bql_query('url', 'contains', '/france/football')
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        self.assertItemsEqual(results, [{'id': 1}])

        # this query should also succeed
        bql_query = get_simple_bql_query('url', 'contains', 'france/footb')
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        self.assertItemsEqual(results, [{'id': 1}])

    def test_ends_query(self):
        bql_query = get_simple_bql_query('url', 'ends', 'article-s.html')
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        self.assertItemsEqual(results, [{'id': 3}])

        bql_query = get_simple_bql_query('url', 'ends', 'main')
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        self.assertItemsEqual(results, [])

    def test_error_link_query(self):
        bql_query = get_simple_bql_query('error_links.3xx.nb', 'gt', 0,
                                         fields=['error_links.3xx.urls'])
        results = list(Query(*QUERY_ARGS, query=bql_query).results)
        expected = {
            'error_links': {
                '3xx': {
                    'urls': [
                        u'http://www.mysite.com/france/football/abcde/main.html',
                        u'http://www.mysite.com/football/france/abc/abcde',
                        u'http://www.mysite.com/football/france/abc/abcde',
                    ]
                }
            }
        }
        self.assertItemsEqual(results, [expected])

    def test_redirects_from_query(self):
        bql_query = get_simple_bql_query('redirects_from_nb', 'gt', 0,
                                         fields=['redirects_from'])
        results = list(Query(*QUERY_ARGS, query=bql_query).results)

        print results


