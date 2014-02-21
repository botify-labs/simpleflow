import unittest
import copy
from mock import MagicMock

from cdf.query.result_transformer import (IdToUrlTransformer,
                                          DefaultValueTransformer,
                                          ExternalUrlTransformer)


ELASTICSEARCH_INDEX = 'mock'
CRAWL_ID = 1
CRAWL_NAME = 'crawl_%d' % CRAWL_ID
REVISION_ID = 1

_MOCK_URL_MAPPING = {
    '1:1': 'url1',
    '1:2': 'url2',
    '1:3': 'url3',
    '1:4': 'url4',
    '1:5': 'url5'
}


def _mock_es_mget(**kwargs):
    assert 'body' in kwargs
    body = kwargs['body']

    docs = []
    for _id in body['ids']:
        url = _MOCK_URL_MAPPING.get(_id, '')
        fields = {
            u'url': url,
            u'http_code': 0
        }

        crt_doc = {
            u'_type': CRAWL_NAME,
            u'exists': True,
            u'_index': ELASTICSEARCH_INDEX,
            u'fields': fields,
            u'_version': 1,
            u'_id': _id
        }
        docs.append(crt_doc)

    result = {
        u'docs': docs
    }
    return result


class TestResultTransformer(unittest.TestCase):
    def setUp(self):
        mock_conn = MagicMock()
        mock_conn.mget = _mock_es_mget
        self.es_conn = mock_conn

    def tearDown(self):
        pass

    def test_id_to_url(self):
        es_result = {
            'error_links': {
                '3xx': {
                    'nb': 3,
                    'urls': [1, 2, 3]
                },
                '5xx': {
                    'nb': 4,
                    'urls': [1, 3, 4, 5]
                }
            }
        }

        # partial transformation, controled by `fields` param
        test_input = copy.deepcopy(es_result)
        trans = IdToUrlTransformer(fields=['error_links.5xx'], es_result=[test_input],
                                   es_conn=self.es_conn, es_index=None,
                                   es_doctype=None, crawl_id=CRAWL_ID)
        trans.transform()

        expected = {
            'error_links': {
                '3xx': {
                    'nb': 3,
                    'urls': [1, 2, 3]
                },
                '5xx': {
                    'nb': 4,
                    'urls': ['url1', 'url3', 'url4', 'url5']
                }
            }
        }

        self.assertDictEqual(expected, test_input)

        # children fields transformation
        test_input = copy.deepcopy(es_result)
        trans = IdToUrlTransformer(fields=['error_links'], es_result=[test_input],
                                   es_conn=self.es_conn, es_index=None,
                                   es_doctype=None, crawl_id=CRAWL_ID)
        trans.transform()

        expected = {
            'error_links': {
                '3xx': {
                    'nb': 3,
                    'urls': ['url1', 'url2', 'url3']
                },
                '5xx': {
                    'nb': 4,
                    'urls': ['url1', 'url3', 'url4', 'url5']
                }
            }
        }

        self.assertDictEqual(expected, test_input)


class TestDefaultValueTransformer(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple(self):
        fields = ['metadata_nb.title']
        results = [{}, {}]
        d = DefaultValueTransformer(results, fields=fields)
        d.transform()

        self.assertEqual(results, [{'metadata_nb': {'title': 0}},
                                   {'metadata_nb': {'title': 0}}])

    def test_children(self):
        fields = ['metadata_nb']
        results = [{}]
        d = DefaultValueTransformer(results, fields=fields)
        d.transform()

        expected = [{'metadata_nb': {'title': 0, 'h1': 0,
                                     'h2': 0, 'description': 0}}]

        self.assertEqual(d.results, expected)


class TestExternalUrlTransformer(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple(self):
        fields = ['canonical_to']
        results = [{'canonical_to': {'url': 'external', 'crawled': True}}]
        d = ExternalUrlTransformer(results, fields=fields)
        d.transform()

        expected = [{'canonical_to': {'url': 'external', 'crawled': False}}]

        self.assertEqual(d.results, expected)


class TestUnindexedUrlTransformer(unittest.TestCase):
    def test_skip_entries(self):
        es_result = {
            'error_links': {
                '3xx': {
                    'nb': 1,
                    'urls': [1]
                }
            }
        }

        es_mock_conn = MagicMock()
        es_mock_conn.mget.return_value = {
            u'docs': [
                {
                    #some other values are also returned by
                    #elasticsearch but they are not relevant for the test
                    u'exists': False,
                }]

        }

        # partial transformation, controled by `fields` param
        test_input = copy.deepcopy(es_result)
        trans = IdToUrlTransformer(fields=['error_links.3xx'], es_result=[test_input],
                                   es_conn=es_mock_conn, es_index=None,
                                   es_doctype=None, crawl_id=CRAWL_ID)
        result = trans.transform()

        #we should have skipped the unexisting doc
        #please note the the 'nb' field is no more equal to the list lenght
        expected = [{'error_links': {'3xx': {'nb': 1, 'urls': []}}}]
        self.assertEqual(expected, result)

    def test_outlinks_internal_not_raise(self):
        es_result = {
            'outlinks_internal': [
                [1, 2, 100], # follow
                [2, 7, 1], # link, meta, robots
            ]
        }

        es_mock_conn = MagicMock()
        es_mock_conn.mget.return_value = {
            u'docs': [
                {
                    "_id": "1:1",
                    "fields": {
                        u'url': "url1",
                        u'http_code': 200,
                        'crawled': True
                    },
                    #some other values are also returned by
                    #elasticsearch but they are not relevant for the test
                    u'exists': True,
                },
                {
                    #some other values are also returned by
                    #elasticsearch but they are not relevant for the test
                    u'exists': False,
                }]
        }

        # partial transformation, controled by `fields` param
        test_input = copy.deepcopy(es_result)
        trans = IdToUrlTransformer(fields=['outlinks_internal'], es_result=[test_input],
                                   es_conn=es_mock_conn, es_index=None,
                                   es_doctype=None, crawl_id=CRAWL_ID)
        result = trans.transform()
        expected = [{
                        'outlinks_internal': [
                            {'url': {'url': 'url1', 'crawled': True},
                             'status': ['nofollow_meta'],
                             'nb_links': 100}
                        ]
                    }]

        self.assertEqual(expected, result)
