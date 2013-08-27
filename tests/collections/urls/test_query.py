# -*- coding:utf-8 -*-
import unittest
import logging
import time

from cdf.log import logger
from cdf.collections.urls.query import Query
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
                'url': 'http://www.mysite.com/',
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
                }
            },
            {
                'id': 2,
                'url': 'http://www.mysite.com/page2.html',
                'http_code': 301,
                'redirects_to': {
                    'url_id': 3
                }
            },
            {
                'id': 3,
                'url': 'http://www.mysite.com/page3.html',
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
                'url': 'http://www.mysite.com/page4.html',
                'http_code': 302,
                'redirects_to': {
                    'url_id': 5
                }
            },
            {
                'id': 5,
                'url': 'http://www.mysite.com/page5.html',
                'http_code': 0
            },
            {
                'id': 6,
                'url': 'http://www.mysite.com/page6.html',
                'http_code': 302,
                'redirects_to': {
                    'url': 'http://www.youtube.com/'
                }
            },
        ]
        self.es = ElasticSearch(ELASTICSEARCH_LOCATION)
        try:
            self.es.delete_index(ELASTICSEARCH_INDEX)
        except:
            pass
        self.es.create_index(ELASTICSEARCH_INDEX)
        for url in urls:
            self.es.index(ELASTICSEARCH_INDEX, "crawl_{}".format(CRAWL_ID), url, url['id'])

        while self.es.count({}, index=ELASTICSEARCH_INDEX)['count'] < len(urls):
            time.sleep(0.1)

        self.query_args = (ELASTICSEARCH_LOCATION, ELASTICSEARCH_INDEX, CRAWL_ID, REVISION_ID)

    def tearDown(self):
        #self.es.delete_index(ELASTICSEARCH_INDEX)
        pass

    def test_count(self):
        # A query with no filter should return 4 results (id=5 should not be returned as it has
        # not been crawled (only exists to return the value of id=4's redirect
        q = Query(*self.query_args, query={})
        self.assertEquals(q.count, 4)

    def test_simple_filter(self):
        query = {
            "fields": ['id', 'url'],
            "filters": {"field": "http_code", "value": 200},
            "sort": ["id"]
        }
        expected_results = [
            {
                'url': u'http://www.mysite.com/',
                'id': 1
            },
            {
                'url': u'http://www.mysite.com/page3.html',
                'id': 3
            }
        ]
        q = Query(*self.query_args, query=query)
        self.assertEquals(q.count, 2)
        self.assertEquals(list(q.results), expected_results)

    def test_and_filter(self):
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
        q = Query(*self.query_args, query=query)
        self.assertEquals([k['id'] for k in q.results], [1])

    def test_or_filter(self):
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
        q = Query(*self.query_args, query=query)
        self.assertEquals([k['id'] for k in q.results], [1, 2, 3])

    def test_redirects_to_crawled(self):
        query = {
            "fields": ['id', 'redirects_to'],
            "filters": {
                'and': [
                    {"field": "http_code", "value": 301},
                    {"field": "redirects_to", "predicate": "not_null"}
                ]
            }
        }
        expected_url = {
            "id": 2,
            "redirects_to": {
                "url": u"http://www.mysite.com/page3.html",
                "crawled": True
            }
        }
        q = Query(*self.query_args, query=query)
        self.assertEquals(q.count, 1)
        self.assertEquals(list(q.results)[0], expected_url)

    def test_redirects_to_not_crawled(self):
        query = {
            "fields": ['id', 'redirects_to'],
            "filters": {
                'and': [
                    {"field": "http_code", "value": 302},
                    {"field": "redirects_to", "predicate": "not_null"}
                ]
            }
        }
        expected_url_4 = {
            "id": 4,
            "redirects_to": {
                "url": u"http://www.mysite.com/page5.html",
                "crawled": False
            }
        }
        expected_url_6 = {
            "id": 6,
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
            "fields": ['id', 'redirects_from'],
            "filters": {
                'and': [
                    {"field": "id", "value": 3},
                    {"field": "redirects_from", "predicate": "not_null"}
                ]
            }
        }
        expected_url = {
            "id": 3,
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

    def test_filters(self):
        filters = {
            "and": {
                "or": [
                    {"field": "resource_type", "predicate": "starts", "value": "recette/"},
                    #{"field": "resource_type", "predicate": "ends", "value": "permalink"},
                ]
            }
        }
        r = UrlRequest(ELASTICSEARCH_LOCATION, ELASTICSEARCH_INDEX, CRAWL_ID, REVISION_ID)
        import json
        print json.dumps(r._make_raw_tagging_filters(filters))
        
        """
        query = {
            "fields": ["url", "resource_type", "metadata.h1"],
            "tagging_filters": {
                "and": [
                    {"not": True, "field": "resource_type", "predicate": "starts", "value": "recette/"}
                ]
            },
            "filters": {
                "and": [
                    {"field": "metadata.h1", "value": "tomate"}
                ]
            }
        }
        print json.dumps(r.make_raw_query(query))
        print r.query(query, start=0, limit=3)
        """
