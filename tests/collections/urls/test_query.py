# -*- coding:utf-8 -*-
import unittest
import logging
import time

from cdf.log import logger
from cdf.collections.urls.query import Query
from cdf.constants import URLS_DATA_MAPPING
from cdf.streams.masks import list_to_mask
from pyelasticsearch import ElasticSearch

ELASTICSEARCH_LOCATION = "http://localhost:9200"
ELASTICSEARCH_INDEX = "cdf_test"
CRAWL_ID = 1
REVISION_ID = 1

logger.setLevel(logging.DEBUG)


class TestQuery(unittest.TestCase):

    def setUp(self):
        urls = [
            {
                'id': 1,
                '_id': "%d:%d" % (CRAWL_ID, 1),
                'crawl_id': CRAWL_ID,
                'url': u'http://www.mysite.com/',
                'http_code': 200,
                'delay2': 100,
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
                "metadata_duplicate": {
                    "h1": [7]
                },
                "metadata_duplicate_nb": {
                    "h1": 1
                },
                "outlinks_internal_nb": {
                    "total": 4,
                    "follow": 3,
                    "follow_unique": 3,
                    "nofollow": 1,
                    "nofollow_combinations": [
                        {"key": ["link"],
                         "value": 1}
                    ],
                },
                "outlinks_internal": [
                    [2, list_to_mask(['follow']), 1],
                    [3, list_to_mask(['follow']), 1],
                    [5, list_to_mask(['follow']), 1],
                    [3, list_to_mask(['link']), 1]
                ],
                "canonical_to": {
                    "url_id": 2,
                },
                "tagging": [
                    {
                        "rev_id": 1,
                        "resource_type": "homepage"
                    }
                ]
            },
            {
                'id': 2,
                '_id': "%d:%d" % (CRAWL_ID, 2),
                'crawl_id': CRAWL_ID,
                'url': u'http://www.mysite.com/page2.html',
                'http_code': 301,
                'redirects_to': {
                    'url_id': 3
                },
                'canonical_from': [1],
                'tagging': [
                    {
                        'rev_id': 1,
                        'resource_type': 'not homepage'
                    }
                ]
            },
            {
                'id': 3,
                '_id': "%d:%d" % (CRAWL_ID, 3),
                'crawl_id': CRAWL_ID,
                'url': u'http://www.mysite.com/page3.html',
                'http_code': 200,
                'redirects_from': [{
                    'http_code': 301,
                    'url_id': 2
                }],
                'metadata_nb': {
                    'title': 0,
                    'h1': 0,
                    'h2': 0,
                    'description': 0
                }
            },
            {
                'id': 4,
                '_id': "%d:%d" % (CRAWL_ID, 4),
                'crawl_id': CRAWL_ID,
                'url': u'http://www.mysite.com/page4.html',
                'http_code': 302,
                'redirects_to': {
                    'url_id': 5
                }
            },
            {
                'id': 5,
                '_id': "%d:%d" % (CRAWL_ID, 5),
                'crawl_id': CRAWL_ID,
                'url': u'http://www.mysite.com/page5.html',
                'http_code': 0
            },
            {
                'id': 6,
                '_id': "%d:%d" % (CRAWL_ID, 6),
                'crawl_id': CRAWL_ID,
                'url': u'http://www.mysite.com/page6.html',
                'http_code': 302,
                'redirects_to': {
                    'url': 'http://www.youtube.com/'
                }
            },
            {
                'id': 7,
                '_id': "%d:%d" % (CRAWL_ID, 7),
                'crawl_id': CRAWL_ID,
                'url': u'http://www.mysite.com/page7.html',
                'http_code': 200,
                'metadata': {
                    'title': ['My title'],
                    'h1': ['Welcome to our website']
                }
            },
        ]
        self.es = ElasticSearch(ELASTICSEARCH_LOCATION)
        try:
            self.es.delete_index(ELASTICSEARCH_INDEX)
        except:
            pass
        self.es.create_index(ELASTICSEARCH_INDEX)

        self.es.put_mapping(ELASTICSEARCH_INDEX, "crawl_{}".format(CRAWL_ID), URLS_DATA_MAPPING)
        for url in urls:
            self.es.index(ELASTICSEARCH_INDEX, "crawl_{}".format(CRAWL_ID), url, url['_id'])

        while self.es.count({}, index=ELASTICSEARCH_INDEX)['count'] < len(urls):
            time.sleep(0.1)

        self.query_args = (ELASTICSEARCH_LOCATION, ELASTICSEARCH_INDEX, "crawl_{}".format(CRAWL_ID), CRAWL_ID, REVISION_ID)

    def tearDown(self):
        #self.es.delete_index(ELASTICSEARCH_INDEX)
        pass

    def test_count(self):
        # A query with no filter should return 4 results (id=5 should not be returned as it has
        # not been crawled (only exists to return the value of id=4's redirect
        q = Query(*self.query_args, query={})
        self.assertEquals(q.count, 6)

    def test_simple_filter(self):
        query = {
            "fields": ['_id', 'url'],
            "filters": {"field": "http_code", "value": 200},
            "sort": ["id"]
        }
        expected_results = [
            {
                'url': 'http://www.mysite.com/',
                '_id': "%d:%d" % (CRAWL_ID, 1)
            },
            {
                'url': 'http://www.mysite.com/page3.html',
                '_id': "%d:%d" % (CRAWL_ID, 3)
            },
            {
                'url': 'http://www.mysite.com/page7.html',
                '_id': "%d:%d" % (CRAWL_ID, 7)
            }
        ]
        q = Query(*self.query_args, query=query)
        self.assertEquals(q.count, 3)

        self.assertEquals(list(q.results), expected_results)

    def test_and_filter(self):
        query = {
            "fields": ['_id'],
            "filters": {
                "and": [
                    {"field": "http_code", "value": 200},
                    {"field": "delay2", "value": 100, "predicate": "gte"},
                ]
            },
            "sort": ["id"]
        }
        q = Query(*self.query_args, query=query)
        self.assertEquals([k['_id'] for k in q.results], ["1:1"])

    def test_or_filter(self):
        query = {
            "fields": ['_id'],
            "filters": {
                "or": [
                    {"field": "http_code", "value": 200},
                    {"field": "http_code", "value": 301},
                ]
            },
            "sort": ["id"]
        }
        q = Query(*self.query_args, query=query)
        self.assertEquals([k['_id'] for k in q.results], ["1:1", "1:2", "1:3", "1:7"])

    def test_redirects_to_crawled(self):
        query = {
            "fields": ['_id', 'redirects_to'],
            "filters": {
                'and': [
                    {"field": "http_code", "value": 301},
                    {"field": "redirects_to", "predicate": "not_null"}
                ]
            }
        }
        expected_url = {
            "_id": '%d:%d' % (CRAWL_ID, 2),
            "redirects_to": {
                "url": "http://www.mysite.com/page3.html",
                "crawled": True
            }
        }
        q = Query(*self.query_args, query=query)
        self.assertEquals(q.count, 1)
        self.assertEquals(list(q.results)[0], expected_url)

    def test_redirects_to_not_crawled(self):
        query = {
            "fields": ['_id', 'redirects_to'],
            "filters": {
                'and': [
                    {"field": "http_code", "value": 302},
                    {"field": "redirects_to", "predicate": "not_null"}
                ]
            }
        }
        expected_url_4 = {
            "_id": "%d:%d" % (CRAWL_ID, 4),
            "redirects_to": {
                "url": u"http://www.mysite.com/page5.html",
                "crawled": False
            }
        }
        expected_url_6 = {
            "_id": "%d:%d" % (CRAWL_ID, 6),
            "redirects_to": {
                "url": u"http://www.youtube.com/",
                "crawled": False
            }
        }
        q = Query(*self.query_args, query=query, sort=('id',))
        self.assertEquals(q.count, 2)
        self.assertEquals(list(q.results)[0], expected_url_4)
        self.assertEquals(list(q.results)[1], expected_url_6)

    def test_redirects_from(self):
        query = {
            "fields": ['_id', 'redirects_from'],
            "filters": {
                'and': [
                    {"field": "_id", "value": "%d:%d" % (CRAWL_ID, 3)},
                    {"field": "redirects_from", "predicate": "not_null"}
                ]
            }
        }
        expected_url = {
            "_id": "%d:%d" % (CRAWL_ID, 3),
            "redirects_from": [{
                "http_code": 301,
                "url": {
                    "url": u"http://www.mysite.com/page2.html",
                    "crawled": True
                }
            }]
        }
        q = Query(*self.query_args, query=query, sort=('id',))
        self.assertEquals(list(q.results)[0], expected_url)

    def test_subfield(self):
        query = {
            "fields": ["metadata.title", "metadata_nb"],
            "filters": {
                "field": "id",
                "value": 2,
                "predicate": "lte"
            }
        }
        q = Query(*self.query_args, query=query, sort=('id',))
        expected_result_1 = {
            "metadata": {
                "title": ["My title"]
            },
            "metadata_nb": {
                "title": 1,
                "h1": 1,
                "description": 0,
                "h2": 0
            }
        }
        results = list(q.results)
        self.assertEquals(results[0], expected_result_1)
        # Url 2 has not title but should return a None value
        expected_result_2 = {
            "metadata": {
                "title": []
            },
            "metadata_nb": {
                "title": 0,
                "h1": 0,
                "description": 0,
                "h2": 0
            }
        }
        self.assertEquals(results[1], expected_result_2)

    def test_outlinks(self):
        query = {
            "fields": ["outlinks_internal_nb", "outlinks_internal"],
            "filters": {
                "field": "_id",
                "value": "%d:%d" % (CRAWL_ID, 1)
            }
        }
        q = Query(*self.query_args, query=query, sort=('id',))
        expected_result = {
            "outlinks_internal_nb": {
                "total": 4,
                "follow": 3,
                "nofollow": 1,
                "follow_unique": 3,
                "nofollow_combinations": [
                    {"key": ["link"],
                     "value": 1}
                ]
            },
            "outlinks_internal": [
                {
                    "url": {
                        "url": "http://www.mysite.com/page2.html",
                        "crawled": True
                    },
                    "status": ["follow"],
                    "nb_links": 1
                },
                {
                    "url": {
                        "url": "http://www.mysite.com/page3.html",
                        "crawled": True
                    },
                    "status": ["follow"],
                    "nb_links": 1
                },
                {
                    "url": {
                        "url": "http://www.mysite.com/page5.html",
                        "crawled": False
                    },
                    "status": ["follow"],
                    "nb_links": 1
                },
                {
                    "url": {
                        "url": "http://www.mysite.com/page3.html",
                        "crawled": True
                    },
                    "status": ["nofollow_link"],
                    "nb_links": 1
                },
            ]
        }
        q = Query(*self.query_args, query=query, sort=('id',))
        results = list(q.results)
        self.assertEquals(results[0]["outlinks_internal_nb"], expected_result["outlinks_internal_nb"])
        self.assertEquals(results[0]["outlinks_internal"], expected_result["outlinks_internal"])

    def test_metadata_duplicate(self):
        query = {
            "fields": ["metadata_duplicate_nb", "metadata_duplicate.h1"],
            "filters": {
                "field": "_id",
                "value": "%d:%d" % (CRAWL_ID, 1)
            }
        }
        q = Query(*self.query_args, query=query, sort=('id',))
        expected_result = {
            "metadata_duplicate_nb": {
                "h1": 1,
                "title": 0,
                "description": 0
            },
            "metadata_duplicate": {
                "h1": [
                    {"url": "http://www.mysite.com/page7.html",
                     "crawled": True},
                ]
            }
        }
        q = Query(*self.query_args, query=query, sort=('_id',))
        self.assertEquals(list(q.results)[0], expected_result)

    def test_canonicals(self):
        query = {
            "fields": ["canonical_from", "canonical_to"],
            "filters": {
                "field": "id",
                "value": 1,
                "predicate": "gte"
            }
        }
        q = Query(*self.query_args, query=query, sort=('id',))
        expected_result_1 = {
            "canonical_to": {
                "url": u"http://www.mysite.com/page2.html",
                "crawled": True
            },
            "canonical_from": []
        }
        self.assertEquals(list(q.results)[0], expected_result_1)
        expected_result_2 = {
            "canonical_to": None,
            "canonical_from": [
                {
                    'url': u'http://www.mysite.com/',
                    'crawled': True
                }
            ]
        }
        self.assertEquals(list(q.results)[1], expected_result_2)
