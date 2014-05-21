import unittest
import copy
from mock import MagicMock
from cdf.metadata.url.backend import ELASTICSEARCH_BACKEND, ElasticSearchBackend

from cdf.query.result_transformer import (IdToUrlTransformer,
                                          DefaultValueTransformer,
                                          ExternalUrlNormalizer,
                                          AggregationTransformer)

ELASTICSEARCH_INDEX = 'mock'
CRAWL_ID = 1
CRAWL_NAME = 'crawl_%d' % CRAWL_ID
REVISION_ID = 1
ES_BACKEND = ELASTICSEARCH_BACKEND

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
            u'found': True,
            u'_index': ELASTICSEARCH_INDEX,
            u'_source': fields,
            u'_version': 1,
            u'_id': _id
        }
        docs.append(crt_doc)

    # url 6 not indexed for some reason
    docs.append({u'found': False})

    result = {
        u'docs': docs
    }
    return result


class TestIdToUrlTransformer(unittest.TestCase):
    def setUp(self):
        mock_conn = MagicMock()
        mock_conn.mget = _mock_es_mget
        self.es_conn = mock_conn

    def tearDown(self):
        pass

    def _get_id_url_transformer(self, fields, es_result):
        return IdToUrlTransformer(fields=fields, es_result=es_result,
                                  es_conn=self.es_conn, es_index=None, es_doctype=None,
                                  crawl_id=CRAWL_ID, backend=ES_BACKEND)

    def test_error_links(self):
        es_result = {
            'outlinks_errors': {
                '3xx': {
                    'urls': [1, 2, 3]
                },
                '5xx': {
                    'urls': [1, 3, 4, 5]
                }
            }
        }

        # partial transformation, controled by `fields` param
        test_input = copy.deepcopy(es_result)
        trans = self._get_id_url_transformer(fields=['outlinks_errors.5xx'],
                                             es_result=[test_input])
        trans.transform()

        expected = {
            'outlinks_errors': {
                '3xx': {
                    'urls': [1, 2, 3],
                },
                '5xx': {
                    'urls': ['url1', 'url3', 'url4', 'url5']
                }
            }
        }
        self.assertEqual(test_input, expected)

        # children fields transformation
        test_input = copy.deepcopy(es_result)
        trans = self._get_id_url_transformer(fields=['outlinks_errors'],
                                             es_result=[test_input])
        trans.transform()

        expected = {
            'outlinks_errors': {
                '3xx': {
                    'urls': ['url1', 'url2', 'url3'],
                },
                '5xx': {
                    'urls': ['url1', 'url3', 'url4', 'url5']
                }
            }
        }

        self.assertDictEqual(expected, test_input)

    def test_links(self):
        es_result = {
            'outlinks_internal': {
                # uid, mask, link number
                'urls': [[5, 7, 40]]
            }
        }
        trans = self._get_id_url_transformer(fields=['outlinks_internal.urls'],
                                             es_result=[es_result])
        trans.transform()

        expected = {
            'outlinks_internal': {
                'urls': [
                    {
                        'url': {
                            'url': 'url5',
                            # mock http code = 0
                            'crawled': False,
                        },
                        'status': [
                            'nofollow_robots',
                            'nofollow_meta',
                            'nofollow_link'
                        ]
                    }
                ]
            }
        }
        self.assertDictEqual(expected, es_result)

    def test_metadata_duplicate(self):
        es_result = {
            'metadata': {
                'title': {
                    'duplicates': {
                        'urls': [1, 5]
                    }
                }
            }
        }
        trans = self._get_id_url_transformer(fields=['metadata.title'],
                                             es_result=[es_result])
        trans.transform()

        expected = {
            'metadata': {
                'title': {
                    'duplicates': {
                        'urls': [
                            {'url': 'url1', 'crawled': True},
                            {'url': 'url5', 'crawled': True},
                        ]
                    }
                }
            }
        }
        self.assertDictEqual(expected, es_result)

    def test_metadata_duplicate_empty(self):
        es_result = {'other_info': 1}
        expected = copy.deepcopy(es_result)
        trans = self._get_id_url_transformer(fields=['metadata.h1'],
                                             es_result=[es_result])
        trans.transform()
        self.assertEqual(es_result, expected)

    def test_redirect_from(self):
        es_result = {
            'redirect': {
                'from': {
                    'urls': [[1, 200]]
                }
            }
        }
        trans = self._get_id_url_transformer(fields=['redirect'],
                                             es_result=[es_result])
        trans.transform()

        expected = {
            'redirect': {
                'from': {
                    'urls': [
                        # crawled is always true for redirect from
                        # here http code comes from `redirect.from.urls`, in production
                        # this should be the same code as in resolved pair (url, http_code)
                        {'http_code': 200, 'url': {'url': 'url1', 'crawled': True}},
                    ]
                }
            }
        }

        self.assertDictEqual(expected, es_result)

    def test_canonical_from(self):
        es_result = {
            'canonical': {
                'from': {
                    'urls': [1, 2, 4, 5]
                }
            }
        }
        trans = self._get_id_url_transformer(fields=['canonical'],
                                             es_result=[es_result])
        trans.transform()

        expected = {
            'canonical': {
                'from': {
                    'urls': ['url1', 'url2', 'url4', 'url5']
                }
            }
        }

        self.assertDictEqual(expected, es_result)

    # canonical to external url is handled by ExternalUrlNormalizer
    def test_redirect_to_internal(self):
        es_result = {
            'redirect': {
                'to': {
                    'url': {'url_id': 4, 'http_code': 300}
                },
            }
        }
        trans = self._get_id_url_transformer(fields=['redirect.to'],
                                             es_result=[es_result])
        trans.transform()

        expected = {
            'redirect': {
                'to': {
                    'url': {'url': 'url4', 'crawled': False, 'http_code': 300}
                },
            }
        }

        self.assertDictEqual(expected, es_result)

    # canonical to external url is handled by ExternalUrlNormalizer
    def test_canonical_to_internal(self):
        es_result = {
            'canonical': {
                'to': {
                    'url': {
                        'url_id': 4,
                    }
                },
                'equal': True
            }
        }
        trans = self._get_id_url_transformer(fields=['canonical.to'],
                                             es_result=[es_result])
        trans.transform()

        expected = {
            'canonical': {
                'to': {
                    'url': {'url': 'url4', 'crawled': False},
                },
                'equal': True
            }
        }

        self.assertDictEqual(expected, es_result)


class TestExternalUrlNormalizer(unittest.TestCase):
    def test_redirect_to_external(self):
        es_result = {
            'redirect': {
                'to': {
                    'url': {
                        'url_str': 'www.abc.com',
                        'http_code': 300
                    }
                }
            }
        }
        trans = ExternalUrlNormalizer(fields=['redirect.to'],
                                      es_result=[es_result])
        trans.transform()
        expected = {
            'redirect': {
                'to': {
                    'url': {
                        'url': 'www.abc.com',
                        'crawled': False,
                        'http_code': 300
                    }
                }
            }
        }

        self.assertDictEqual(expected, es_result)


class TestDefaultValueTransformer(unittest.TestCase):
    def setUp(self):
        data_format = {
            'outer.middle.int': {
                'type': 'integer'
            },
            'outer.middle.list': {
                'type': 'string',
                'settings': {'list'}
            },
            'special': {
                'type': 'string',
                'default_value': 'hey!!'
            }
        }
        self.backend = ElasticSearchBackend(data_format)

    def test_int(self):
        fields = ['outer.middle.int']
        results = [{}, {}]
        d = DefaultValueTransformer(results, fields=fields,
                                    backend=self.backend)
        d.transform()
        self.assertEqual(results, [{'outer': {'middle': {'int': 0}}},
                                   {'outer': {'middle': {'int': 0}}}])

    def test_list(self):
        fields = ['outer.middle.list']
        results = [{}]
        d = DefaultValueTransformer(results, fields=fields,
                                    backend=self.backend)
        d.transform()

        expected = [{'outer': {'middle': {'list': []}}}]
        self.assertEqual(d.results, expected)

    def test_special_default_value(self):
        fields = ['special']
        results = [{}]
        d = DefaultValueTransformer(results, fields=fields,
                                    backend=self.backend)
        d.transform()

        expected = [{'special': 'hey!!'}]
        self.assertEqual(d.results, expected)

    def test_multiple_fields(self):
        fields = ['outer.middle.list', 'outer.middle.int']
        results = [{}]
        d = DefaultValueTransformer(results, fields=fields,
                                    backend=self.backend)
        d.transform()

        expected = [{'outer': {'middle': {'list': [], 'int': 0}}}]
        self.assertEqual(d.results, expected)


class TestAggregationResultTransformer(unittest.TestCase):
    def setUp(self):
        self._distinct_result = {
            "queryagg_00": {
                "buckets": [
                    {
                        "key": "a",
                        "doc_count": 10,
                        "metricagg_00": {
                            "value": 10
                        }
                    },
                    {
                        "key": "b",
                        "doc_count": 10,
                        "metricagg_00": {
                            "value": 10
                        }
                    },
                ]
            }
        }

        self._range_result = {
            "queryagg_00": {
                "buckets": [
                    {
                        "to": 50,
                        "doc_count": 2,
                        "key": "*-50",
                        "metricagg_00": {
                            "value": 2
                        }
                    },
                    {
                        "from": 50,
                        "to": 100,
                        "key": "50-100",
                        "doc_count": 4,
                        "metricagg_00": {
                            "value": 4
                        }
                    }
                ]
            }
        }

    def test_distinct_result(self):
        results = self._distinct_result
        d = AggregationTransformer(results)

        expected = [
            {
                "groups": [
                    {
                        "key": ["a"],
                        "metrics": [10]
                    },
                    {
                        "key": ["b"],
                        "metrics": [10]
                    },
                ]
            }
        ]
        self.assertEqual(d.transform(), expected)

    def test_range_result(self):
        results = self._range_result
        d = AggregationTransformer(results)

        expected = [
            {
                "groups": [
                    {
                        "key": [{"to": 50}],
                        "metrics": [2]
                    },
                    {
                        "key": [{"from": 50, "to": 100}],
                        "metrics": [4]
                    }
                ]
            }
        ]
        self.assertEqual(d.transform(), expected)

    def test_empty_result(self):
        results = {}
        d = AggregationTransformer(results)
        d.transform()
        self.assertEqual(results, {})

    def test_sum_op(self):
        sum_result = {
            'queryagg_00': {
                'buckets': [
                    {
                        'key': 'a',
                        'doc_count': 100,
                        'metricagg_00': {
                            'value': 10
                        }
                    },
                    {
                        'key': 'b',
                        'doc_count': 50,
                        'metricagg_00': {
                            'value': 5
                        }
                    }
                ]
            }
        }
        d = AggregationTransformer(sum_result)
        expected = [
            {
                'groups': [
                    {
                        'key': ['a'],
                        'metrics': [10]
                    },
                    {
                        'key': ['b'],
                        'metrics': [5]
                    }
                ]
            }
        ]
        self.assertEqual(d.transform(), expected)

    def test_multiple_aggs(self):
        results = {
            'queryagg_00': {'buckets': [{'key': 'a', 'doc_count': 1, 'metricagg_00': {'value': 1}}]},
            'queryagg_01': {'buckets': [{'key': 'b', 'doc_count': 2, 'metricagg_00': {'value': 2}}]}
        }

        d = AggregationTransformer(results)

        expected = [
            {'groups': [{'key': ['a'], 'metrics': [1]}]},
            {'groups': [{'key': ['b'], 'metrics': [2]}]}
        ]
        self.assertEqual(d.transform(), expected)

    def test_agg_group_nested_fields(self):
        results = {
            'queryagg_00': {
                'buckets': [{
                    'key': 'a',
                    'doc_count': 100,
                    'subagg': {
                        'buckets': [
                            {
                                'key': 'b',
                                'doc_count': 20,
                                'subagg': {
                                    'buckets': [
                                        {'key': 'd',
                                         'doc_count': 5,
                                         'metricagg_00': {'value': 5}},
                                        {'key': 'e',
                                         'doc_count': 15,
                                         'metricagg_00': {'value': 15}},
                                    ]
                                }
                            },
                            {
                                'key': 'c',
                                'doc_count': 80,
                                'subagg': {
                                    'buckets': [
                                        {'key': 'e',
                                         'doc_count': 80,
                                         'metricagg_00': {'value': 80}}
                                    ]
                                }
                            }
                        ]
                    }
                }]
            }
        }

        expected = [
            {'groups': [
                {'key': ['a', 'b', 'd'], 'metrics': [5]},
                {'key': ['a', 'b', 'e'], 'metrics': [15]},
                {'key': ['a', 'c', 'e'], 'metrics': [80]},
            ]}
        ]

        d = AggregationTransformer(results)
        self.assertEqual(d.transform(), expected)
