# -*- coding:utf-8 -*-
import unittest
import logging

from mock import MagicMock

from cdf.log import logger
from cdf.collections.urls.query import Query


ELASTICSEARCH_LOCATION = "http://localhost:9200"
ELASTICSEARCH_INDEX = "cdf_test"
CRAWL_ID = 1
CRAWL_NAME = "crawl_%d" % CRAWL_ID
REVISION_ID = 1

logger.setLevel(logging.DEBUG)


class TestQuery(unittest.TestCase):

    def setUp(self):
        self.query_args = (ELASTICSEARCH_LOCATION,
                           ELASTICSEARCH_INDEX,
                           "crawl_{}".format(CRAWL_ID),
                           CRAWL_ID,
                           REVISION_ID)

    def tearDown(self):
        #self.es.delete_index(ELASTICSEARCH_INDEX)
        pass

    def get_search_expected_results(self, result_ids):

        hits = [
            {
                "_id": "1:1",
                "_index": "cdf_test",
                "_score": None,
                "_source": {
                    "_id": "1:1",
                    "canonical_to": {
                        "url_id": 2
                    },
                    "crawl_id": 1,
                    "delay2": 100,
                    "http_code": 200,
                    "id": 1,
                    "metadata": {
                        "h1": ["Welcome to our website"],
                        "title": ["My title"]
                    },
                    "metadata_duplicate": {
                        "h1": [7]
                    },
                    "metadata_duplicate_nb": {
                        "h1": 1
                    },
                    "metadata_nb": {
                        "description": 0,
                        "h1": 1,
                        "h2": 0,
                        "title": 1
                    },
                    "outlinks_internal": [
                        [2, 0, 1],
                        [3, 0, 1],
                        [5, 0, 1],
                        [3, 1, 1]
                    ],
                    "outlinks_internal_nb": {
                        "follow": 3,
                        "follow_unique": 3,
                        "nofollow": 1,
                        "nofollow_combinations": [
                            {
                                "key": ["link"],
                                "value": 1
                            }
                        ],
                        "total": 4
                    },
                    "tagging": [
                        {
                            "resource_type": "homepage",
                            "rev_id": 1
                        }
                    ],
                    "url": "http://www.mysite.com/"
                },
                "_type": CRAWL_NAME,
                "sort": [1]
            },
            {
                "_id": "1:2",
                "_index": "cdf_test",
                "_score": None,
                "_source": {
                    "_id": "1:2",
                    "canonical_from": [1],
                    "crawl_id": 1,
                    "http_code": 301,
                    "id": 2,
                    "redirects_to": {
                        "url_id": 3
                    },
                    "tagging": [
                        {
                            "resource_type": "not homepage",
                            "rev_id": 1
                        }
                    ],
                    "url": "http://www.mysite.com/page2.html"
                },
                "_type": CRAWL_NAME,
                "sort": [2]
            },
            {
                "_id": "1:3",
                "_index": "cdf_test",
                "_score": None,
                "_source": {
                    "_id": "1:3",
                    "crawl_id": 1,
                    "http_code": 200,
                    "id": 3,
                    "metadata_nb": {
                        "description": 0,
                        "h1": 0,
                        "h2": 0,
                        "title": 0
                    },
                    "redirects_from": [
                        {
                            "http_code": 301,
                            "url_id": 2
                        }
                    ],
                    "url": "http://www.mysite.com/page3.html"
                },
                "_type": CRAWL_NAME,
                "sort": [3]
            },
            {
                "_id": "1:4",
                "_index": "cdf_test",
                "_score": None,
                "_source": {
                    "_id": "1:4",
                    "crawl_id": 1,
                    "http_code": 302,
                    "id": 4,
                    "redirects_to": {
                        "url_id": 5
                    },
                    "url": "http://www.mysite.com/page4.html"
                },
                "_type": CRAWL_NAME,
                "sort": [4]
            },
            {
                "_id": "1:6",
                "_index": "cdf_test",
                "_score": None,
                "_source": {
                    "_id": "1:6",
                    "crawl_id": 1,
                    "http_code": 302,
                    "id": 6,
                    "redirects_to": {
                        "url": "http://www.youtube.com/"
                    },
                    "url": "http://www.mysite.com/page6.html"
                },
                "_type": CRAWL_NAME,
                "sort": [6]
            },
            {
                "_id": "1:7",
                "_index": "cdf_test",
                "_score": None,
                "_source": {
                    "_id": "1:7",
                    "crawl_id": 1,
                    "http_code": 200,
                    "id": 7,
                    "metadata": {
                        "h1": ["Welcome to our website"],
                        "title": ["My title"]
                    },
                    "url": "http://www.mysite.com/page7.html"
                },
                "_type": CRAWL_NAME,
                "sort": [7]
            }
        ]

        l = []
        for id in result_ids:
            for hit in hits:
                if int(hit["_id"].split(":")[1]) == id:
                    l.append(hit)

        result = {
            "_shards": {
                "failed": 0,
                "successful": 5,
                "total": 5
            },
            "hits": {
                "hits": l,
                "max_score": None,
                "total": len(l)
                },
            "timed_out": False,
            "took": 2
            }
        return result

    def get_mget_expected_arguments(self, result_ids):
        """Return the expected arguments for a mget query on the search backend
        result_ids : a list of int representing the expected ids
        """
        result = {
            "body": {'ids': ["%d:%d" % (CRAWL_ID, id) for id in result_ids]},
            "fields": ['url', 'http_code'],
            "index": ELASTICSEARCH_INDEX,
            "doc_type": CRAWL_NAME
        }
        return result

    def test_count(self):
        # A query with no filter should return 4 results (id=5 should not be returned as it has
        # not been crawled (only exists to return the value of id=4's redirect
        search_backend = MagicMock()
        search_backend.search.return_value = self.get_search_expected_results([1, 2, 3, 4, 6, 7])
        q = Query(*self.query_args, query={}, search_backend=search_backend)
        self.assertEquals(q.count, 6)

        expected_elasticsearch_query = {
            'sort': ('id',),
            'filter': {
                'and': [
                    {'range': {'http_code': {'from': 0, 'include_lower': False}}},
                    {'term': {'crawl_id': 1}}]
            }
        }
        search_backend.search.assert_called_with(body=expected_elasticsearch_query,
                                                 doc_type=CRAWL_NAME,
                                                 size=100,
                                                 index=ELASTICSEARCH_INDEX,
                                                 offset=0)

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
        search_backend = MagicMock()
        search_backend.search.return_value = self.get_search_expected_results([1, 3, 7])
        q = Query(*self.query_args, query=query, search_backend=search_backend)

        self.assertEquals(q.count, 3)

        self.assertEquals(list(q.results), expected_results)
        expected_elasticsearch_query = {
            'sort': ['id'],
            'filter': {
                'and': [
                    {'range': {'http_code': {'from': 0, 'include_lower': False}}},
                    {'term': {'crawl_id': 1}}, {'term': {'http_code': 200}}]
            }
        }
        search_backend.search.assert_called_with(body=expected_elasticsearch_query,
                                                 doc_type=CRAWL_NAME,
                                                 size=100,
                                                 index=ELASTICSEARCH_INDEX,
                                                 offset=0)

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
        search_backend = MagicMock()
        search_backend.search.return_value = self.get_search_expected_results([1])
        q = Query(*self.query_args, query=query, search_backend=search_backend)
        self.assertEquals([k['_id'] for k in q.results], ["1:1"])
        expected_elasticsearch_query = {
            'sort': ['id'],
            'filter': {
                'and': [
                    {'term': {'http_code': 200}},
                    {'range': {'delay2': {'from': 100}}},
                    {'range': {'http_code': {'from': 0, 'include_lower': False}}},
                    {'term': {'crawl_id': 1}}]
            }
        }
        search_backend.search.assert_called_with(body=expected_elasticsearch_query,
                                                 doc_type=CRAWL_NAME,
                                                 size=100,
                                                 index=ELASTICSEARCH_INDEX,
                                                 offset=0)

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
        search_backend = MagicMock()
        search_backend.search.return_value = self.get_search_expected_results([1, 2, 3, 7])
        q = Query(*self.query_args, query=query, search_backend=search_backend)
        self.assertEquals([k['_id'] for k in q.results], ["1:1", "1:2", "1:3", "1:7"])

        expected_elasticsearch_query = {
            'sort': ['id'],
            'filter': {
                'and': [
                    {'and': [{'range': {'http_code': {'from': 0, 'include_lower': False}}}, {'term': {'crawl_id': 1}}]},
                    {'or': [{'term': {'http_code': 200}}, {'term': {'http_code': 301}}]}
                ]
            }
        }
        search_backend.search.assert_called_with(body=expected_elasticsearch_query,
                                                 doc_type=CRAWL_NAME,
                                                 size=100,
                                                 index=ELASTICSEARCH_INDEX,
                                                 offset=0)

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
        search_backend = MagicMock()
        search_backend.search.return_value = self.get_search_expected_results([2])
        search_backend.mget.return_value = {
            u'docs': [
                {
                    u'_type': CRAWL_NAME,
                    u'exists': True,
                    u'_index': ELASTICSEARCH_INDEX,
                    u'fields': {u'url': u'http://www.mysite.com/page3.html', u'http_code': 200},
                    u'_version': 1,
                    u'_id': u'1:3'
                }
                ]
            }
        q = Query(*self.query_args, query=query, search_backend=search_backend)
        self.assertEquals(q.count, 1)
        self.assertEquals(list(q.results)[0], expected_url)

        expected_elasticsearch_query = {
            'sort': ('id',),
            'filter': {
                'and': [
                    {'term': {'http_code': 301}},
                    {'or': [{'exists': {'field': 'redirects_to.url'}}, {'exists': {'field': 'redirects_to.url_id'}}, {'exists': {'field': 'redirects_to.http_code'}}]},
                    {'range': {'http_code': {'from': 0, 'include_lower': False}}},
                    {'term': {'crawl_id': 1}}
                ]
            }
        }

        search_backend.search.assert_called_with(body=expected_elasticsearch_query,
                                                 doc_type=CRAWL_NAME,
                                                 size=100,
                                                 index=ELASTICSEARCH_INDEX,
                                                 offset=0)

        search_backend.mget.assert_called_with(**self.get_mget_expected_arguments([3]))

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
        search_backend = MagicMock()
        search_backend.search.return_value = self.get_search_expected_results([4, 6])
        search_backend.mget.return_value = {
            u'docs': [
                {
                    u'_type': CRAWL_NAME,
                    u'exists': True,
                    u'_index': ELASTICSEARCH_INDEX,
                    u'fields': {u'url': u'http://www.mysite.com/page5.html', u'http_code': 0},
                    u'_version': 1,
                    u'_id': u'1:5'
                }
                ]
            }
        q = Query(*self.query_args,
                  query=query, sort=('id',),
                  search_backend=search_backend)
        self.assertEquals(q.count, 2)
        self.assertEquals(list(q.results)[0], expected_url_4)
        self.assertEquals(list(q.results)[1], expected_url_6)

        expected_elasticsearch_query = {
            'sort': ('id',),
            'filter': {
                'and': [
                    {'term': {'http_code': 302}},
                    {'or': [{'exists': {'field': 'redirects_to.url'}},{'exists': {'field': 'redirects_to.url_id'}}, {'exists': {'field': 'redirects_to.http_code'}}]},
                    {'range': {'http_code': {'from': 0, 'include_lower': False}}},
                    {'term': {'crawl_id': 1}}
                ]
            }
        }
        search_backend.search.assert_called_with(body=expected_elasticsearch_query,
                                                 doc_type=CRAWL_NAME,
                                                 size=100,
                                                 index=ELASTICSEARCH_INDEX,
                                                 offset=0)
        search_backend.mget.assert_called_with(**self.get_mget_expected_arguments([5]))

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
        search_backend = MagicMock()
        search_backend.search.return_value = self.get_search_expected_results([3])
        search_backend.mget.return_value={
            u'docs': [
                {
                    u'_type': CRAWL_NAME,
                    u'exists': True,
                    u'_index': ELASTICSEARCH_INDEX,
                    u'fields': {
                        u'url': u'http://www.mysite.com/page2.html',
                        u'http_code': 301
                    },
                    u'_version': 1,
                    u'_id': u'1:2'
                }
                ]
            }
        q = Query(*self.query_args,
                  query=query, sort=('id',),
                  search_backend=search_backend)
        self.assertEquals(list(q.results)[0], expected_url)

        expected_elasticsearch_query = {
            'sort': ('id',),
            'filter': {
                'and': [
                    {'term': {'_id': '1:3'}},
                    {'or': [{'exists': {'field': 'redirects_from.url_id'}}, {'exists': {'field': 'redirects_from.http_code'}}]},
                    {'range': {'http_code': {'from': 0, 'include_lower': False}}},
                    {'term': {'crawl_id': 1}}
                ]
            }
        }
        search_backend.search.assert_called_with(body=expected_elasticsearch_query,
                                                 doc_type=CRAWL_NAME,
                                                 size=100,
                                                 index=ELASTICSEARCH_INDEX,
                                                 offset=0)

        search_backend.mget.assert_called_with(**self.get_mget_expected_arguments([2]))

    def test_subfield(self):
        query = {
            "fields": ["metadata.title", "metadata_nb"],
            "filters": {
                "field": "id",
                "value": 2,
                "predicate": "lte"
            }
        }
        search_backend = MagicMock()
        search_backend.search.return_value = self.get_search_expected_results([1, 2, 3, 4, 5, 7])

        q = Query(*self.query_args, query=query, sort=('id',), search_backend=search_backend)
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

        expected_elasticsearch_query = {
            'sort': ('id',),
            'filter': {
                'and': [
                    {'range': {'http_code': {'from': 0, 'include_lower': False}}},
                    {'term': {'crawl_id': 1}}, {'range': {'id': {'to': 2}}}
                ]
            }
}
        search_backend.search.assert_called_with(body=expected_elasticsearch_query,
                                                 doc_type=CRAWL_NAME,
                                                 size=100,
                                                 index=ELASTICSEARCH_INDEX,
                                                 offset=0)

    def test_outlinks(self):
        query = {
            "fields": ["outlinks_internal_nb", "outlinks_internal"],
            "filters": {
                "field": "_id",
                "value": "%d:%d" % (CRAWL_ID, 1)
            }
        }
        search_backend = MagicMock()
        search_backend.search.return_value = self.get_search_expected_results([1, 2, 3, 4, 5, 7])

        q = Query(*self.query_args, query=query, sort=('id',), search_backend = search_backend)
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

        search_backend2 = MagicMock()
        search_backend2.search.return_value = {
            "_shards": {
                "failed": 0,
                "successful": 5,
                "total": 5
            },
            "hits": {
                "hits": [
                    {
                        "_id": "1:1",
                        "_index": "cdf_test",
                        "_score": None,
                        "_source": {
                            "_id": "1:1",
                            "canonical_to": {
                                "url_id": 2
                            },
                            "crawl_id": 1,
                            "delay2": 100,
                            "http_code": 200,
                            "id": 1,
                            "metadata": {
                                "h1": ["Welcome to our website"],
                                "title": ["My title"]
                            },
                            "metadata_duplicate": {
                                "h1": [7]
                            },
                            "metadata_duplicate_nb": {
                                "h1": 1
                            },
                            "metadata_nb": {
                                "description": 0,
                                "h1": 1,
                                "h2": 0,
                                "title": 1
                            },
                            "outlinks_internal": [
                                [2, 0, 1],
                                [3, 0, 1],
                                [5, 0, 1],
                                [3, 1, 1]
                            ],
                            "outlinks_internal_nb": {
                                "follow": 3,
                                "follow_unique": 3,
                                "nofollow": 1,
                                "nofollow_combinations": [
                                    {
                                        "key": ["link"],
                                        "value": 1
                                    }
                                ],
                                "total": 4
                            },
                            "tagging": [
                                {
                                    "resource_type": "homepage",
                                    "rev_id": 1
                                }
                            ],
                            "url": "http://www.mysite.com/"
                        },
                        "_type": CRAWL_NAME,
                        "sort": [1]
                    }
                ],
                "max_score": None,
                "total": 1
            },
            "timed_out": False,
            "took": 2
        }
        search_backend2.mget.return_value = {
            u'docs': [
                {u'_type': CRAWL_NAME, u'exists': True, u'_index': ELASTICSEARCH_INDEX, u'fields': {u'url': u'http://www.mysite.com/page2.html', u'http_code': 301}, u'_version': 1, u'_id': u'1:2'},
                {u'_type': CRAWL_NAME, u'exists': True, u'_index': ELASTICSEARCH_INDEX, u'fields': {u'url': u'http://www.mysite.com/page3.html', u'http_code': 200}, u'_version': 1, u'_id': u'1:3'},
                {u'_type': CRAWL_NAME, u'exists': True, u'_index': ELASTICSEARCH_INDEX, u'fields': {u'url': u'http://www.mysite.com/page5.html', u'http_code': 0}, u'_version': 1, u'_id': u'1:5'}
            ]
        }


        q = Query(*self.query_args,
                  query=query,
                  sort=('id',),
                  search_backend=search_backend2)
        results = list(q.results)
        self.assertEquals(results[0]["outlinks_internal_nb"], expected_result["outlinks_internal_nb"])
        self.assertEquals(results[0]["outlinks_internal"], expected_result["outlinks_internal"])

        expected_elasticsearch_query = {
            'sort': ('id',),
            'filter': {'and': [{'range': {'http_code': {'from': 0, 'include_lower': False}}}, {'term': {'crawl_id': 1}}, {'term': {'_id': '1:1'}}]}}
        search_backend2.search.assert_called_with(body=expected_elasticsearch_query,
                                                  doc_type=CRAWL_NAME,
                                                  size=100,
                                                  index=ELASTICSEARCH_INDEX,
                                                  offset=0)
        search_backend2.mget.assert_called_with(**self.get_mget_expected_arguments([2, 3, 5]))

    def test_metadata_duplicate(self):
        query = {
            "fields": ["metadata_duplicate_nb", "metadata_duplicate.h1"],
            "filters": {
                "field": "_id",
                "value": "%d:%d" % (CRAWL_ID, 1)
            }
        }
        search_backend = MagicMock()
        search_backend.search.return_value = self.get_search_expected_results([1, 2, 3, 4, 5, 7])
        q = Query(*self.query_args, query=query, sort=('id',), search_backend=search_backend)
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

        search_backend.mget.return_value = {
            u'docs': [
                {
                    u'_type': CRAWL_NAME,
                    u'exists': True,
                    u'_index': ELASTICSEARCH_INDEX,
                    u'fields': {
                        u'url': u'http://www.mysite.com/page7.html',
                        u'http_code': 200
                    },
                    u'_version': 1,
                    u'_id': u'1:7'
                }
                ]
            }

        q = Query(*self.query_args, query=query, sort=('_id',), search_backend=search_backend)
        self.assertEquals(list(q.results)[0], expected_result)

        expected_elasticsearch_query = {
            'sort': ('id',),
            'filter': {
                'and': [
                    {'range': {'http_code': {'from': 0, 'include_lower': False}}},
                    {'term': {'crawl_id': 1}}, {'term': {'_id': '1:1'}}
                ]
            }
        }
        search_backend.search.assert_called_with(body=expected_elasticsearch_query,
                                                 doc_type=CRAWL_NAME,
                                                 size=100,
                                                 index=ELASTICSEARCH_INDEX,
                                                 offset=0)

        search_backend.mget.assert_called_with(**self.get_mget_expected_arguments([7]))

    def test_canonicals(self):
        query = {
            "fields": ["canonical_from", "canonical_to"],
            "filters": {
                "field": "id",
                "value": 1,
                "predicate": "gte"
            }
        }
        search_backend = MagicMock()
        search_backend.search.return_value = self.get_search_expected_results([1, 2])
        search_backend.mget.return_value = {
            u'docs': [
                {
                    u'_type': CRAWL_NAME,
                    u'exists': True,
                    u'_index': ELASTICSEARCH_INDEX,
                    u'fields': {
                        u'url': u'http://www.mysite.com/',
                        u'http_code': 200
                    },
                    u'_version': 1,
                    u'_id': u'1:1'
                },
                {
                    u'_type': CRAWL_NAME,
                    u'exists': True,
                    u'_index': ELASTICSEARCH_INDEX,
                    u'fields': {u'url': u'http://www.mysite.com/page2.html', u'http_code': 301},
                    u'_version': 1,
                    u'_id': u'1:2'
                }
                ]
            }

        q = Query(*self.query_args, query=query, sort=('id',),
                  search_backend=search_backend)
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

        expected_elasticsearch_query = {
            'sort': ('id',),
            'filter': {
                'and': [
                    {'range': {'http_code': {'from': 0, 'include_lower': False}}},
                    {'term': {'crawl_id': 1}}, {'range': {'id': {'from': 1}}}
                ]
            }
        }
        search_backend.search.assert_called_with(body=expected_elasticsearch_query,
                                                 doc_type=CRAWL_NAME,
                                                 size=100,
                                                 index=ELASTICSEARCH_INDEX,
                                                 offset=0)

        search_backend.mget.assert_called_with(**self.get_mget_expected_arguments([1, 2]))
