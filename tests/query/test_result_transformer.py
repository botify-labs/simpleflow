import unittest
import copy
from mock import MagicMock

from cdf.compat import json
from cdf.testing.es_mock import get_es_mget_mock
from cdf.metadata.url.backend import (
    ElasticSearchBackend
)
from cdf.metadata.url.url_metadata import ES_LIST
from cdf.query.result_transformer import (
    IdToUrlTransformer,
    DefaultValueTransformer,
    ExternalUrlNormalizer,
    AggregationTransformer,
    RedirectToStrategy,
    EmptyHtmlExtractTransformer)
from cdf.query.result_transformer import (
    ErrorLinkStrategy,
    LinksStrategy,
    ContextAwareMetaDuplicationStrategy,
    MetaDuplicateStrategy,
    CanonicalFromStrategy,
    CanonicalToStrategy,
    RedirectFromStrategy,
    HrefLangStrategy
)
from cdf.testing.fixtures.dataformat import DATA_FORMAT_FIXTURE

CRAWL_ID = 1
ES_BACKEND = ElasticSearchBackend(DATA_FORMAT_FIXTURE)


class TestUrlIdResolutionStrategies(unittest.TestCase):
    def setUp(self):
        self.id_to_url = {
            1: ['url1', 0],
            2: ['url2', 0],
            3: ['url3', 0],
            4: ['url4', 0],
            5: ['url5', 0],
            6: ['url6', 200]
        }

    def test_error_links(self):
        strat = ErrorLinkStrategy('toto')
        es_result = {
            'outlinks_errors': {
                'toto': {
                    'urls': [1, 2]
                },
            }
        }
        expected_extract = [1, 2]
        expected_transform = {
            'outlinks_errors': {
                'toto': {
                    'urls': ['url1', 'url2']
                },
            }
        }

        self.assertEqual(strat.extract(es_result), expected_extract)
        self.assertEqual(strat.transform(es_result, self.id_to_url),
                         expected_transform)

    def test_links(self):
        strat = LinksStrategy('outlinks_internal')
        es_result = {
            'outlinks_internal': {
                # uid, mask
                'urls': [[5, 7]]
            }
        }

        expected_extract = [5]
        expected_transform = {
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

        self.assertEqual(strat.extract(es_result), expected_extract)
        self.assertEqual(strat.transform(es_result, self.id_to_url),
                         expected_transform)

    def test_meta_duplication(self):
        strat = MetaDuplicateStrategy('toto')
        es_result = {
            'metadata': {
                'toto': {
                    'duplicates': {
                        'urls': [1, 5]
                    }
                }
            }
        }

        expected_extract = [1, 5]
        expected_transform = {
            'metadata': {
                'toto': {
                    'duplicates': {
                        'urls': [
                            {'url': 'url1', 'crawled': True},
                            {'url': 'url5', 'crawled': True},
                        ]
                    }
                }
            }
        }

        self.assertEqual(strat.extract(es_result), expected_extract)
        self.assertEqual(strat.transform(es_result, self.id_to_url),
                         expected_transform)

    def test_context_aware_duplication(self):
        strat = ContextAwareMetaDuplicationStrategy('toto')
        es_result = {
            'metadata': {
                'toto': {
                    'duplicates': {
                        'context_aware': {
                            'urls': [1, 5]
                        }
                    }
                }
            }
        }

        expected_extract = [1, 5]
        expected_transform = {
            'metadata': {
                'toto': {
                    'duplicates': {
                        'context_aware': {
                            'urls': [
                                {'url': 'url1', 'crawled': True},
                                {'url': 'url5', 'crawled': True},
                            ]
                        }
                    }
                }
            }
        }

        self.assertEqual(strat.extract(es_result), expected_extract)
        self.assertEqual(strat.transform(es_result, self.id_to_url),
                         expected_transform)

    # canonical to external url is handled by ExternalUrlNormalizer
    def test_canonical_to(self):
        strat = CanonicalToStrategy()
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

        expected_extract = [4]
        expected_transform = {
            'canonical': {
                'to': {
                    'url': {'url': 'url4', 'crawled': False},
                },
                'equal': True
            }
        }

        self.assertEqual(strat.extract(es_result), expected_extract)
        self.assertEqual(strat.transform(es_result, self.id_to_url),
                         expected_transform)

    def test_canonial_from(self):
        strat = CanonicalFromStrategy()
        es_result = {
            'canonical': {
                'from': {
                    'urls': [1, 2, 4, 5]
                }
            }
        }

        expected_extract = [1, 2, 4, 5]
        expected_transform = {
            'canonical': {
                'from': {
                    'urls': ['url1', 'url2', 'url4', 'url5']
                }
            }
        }

        self.assertEqual(strat.extract(es_result), expected_extract)
        self.assertEqual(strat.transform(es_result, self.id_to_url),
                         expected_transform)

    # redirection to external url is handled by ExternalUrlNormalizer
    def test_redirect_to(self):
        strat = RedirectToStrategy()
        es_result = {
            'redirect': {
                'to': {
                    'url': {'url_id': 4, 'http_code': 300}
                },
            }
        }

        expected_extract = [4]
        expected_transform = {
            'redirect': {
                'to': {
                    'url': {'url': 'url4', 'crawled': False}
                },
            }
        }

        self.assertEqual(strat.extract(es_result), expected_extract)
        self.assertEqual(strat.transform(es_result, self.id_to_url),
                         expected_transform)

    def test_redirect_from(self):
        strat = RedirectFromStrategy()
        es_result = {
            'redirect': {
                'from': {
                    'urls': [[1, 200]]
                }
            }
        }

        expected_extract = [1]
        expected_transform = {
            'redirect': {
                'from': {
                    'urls': [
                        # crawled is always true for redirect from
                        # here http code comes from `redirect.from.urls`, in production
                        # this should be the same code as in resolved pair (url, http_code)
                        ['url1', 200]
                    ]
                }
            }
        }

        self.assertEqual(strat.extract(es_result), expected_extract)
        self.assertEqual(strat.transform(es_result, self.id_to_url),
                         expected_transform)

    def test_hreflang(self):
        strat = HrefLangStrategy('in.not_valid')
        values = [
            {"url": "http://www.site.com/", "value": "zz", "errors": ["DEST_BLOCKED_CONFIG"]},
            {"url_id": 6, "value": "zz", "errors": ["LANG_NOT_RECOGNIZED"]}
        ]
        dumped_values = json.dumps(values)
        es_result = {
            "rel": {
                "hreflang": {
                    "in": {
                        "not_valid": {
                            "values": dumped_values
                        }
                    }
                }
            }
        }
        expected_extract = [6]
        transformed_values = [
            {"url": {"url": "http://www.site.com/", "crawled": False}, "value": "zz", "errors": ["DEST_BLOCKED_CONFIG"]},
            {"url": {"url": "url6", "crawled": True}, "value": "zz", "errors": ["LANG_NOT_RECOGNIZED"]}
        ]

        expected_transform = {
            "rel": {"hreflang": {"in": {"not_valid": {"values": transformed_values }}}}
        }
        self.assertEqual(strat.extract(es_result), expected_extract)
        self.assertEqual(strat.transform(es_result, self.id_to_url),
                         expected_transform)

    def test_previous(self):
        strat = ErrorLinkStrategy('toto', 'previous.')
        es_result = {
            'previous': {
                'outlinks_errors': {
                    'toto': {
                        'urls': [1, 2]
                    },
                }
            }
        }
        expected_extract = [1, 2]
        expected_transform = {
            'previous': {
                'outlinks_errors': {
                    'toto': {
                        'urls': ['url1', 'url2']
                    },
                }
            }
        }

        self.assertEqual(strat.extract(es_result), expected_extract)
        self.assertEqual(strat.transform(es_result, self.id_to_url),
                         expected_transform)


class TestIdToUrlTransformer(unittest.TestCase):
    def setUp(self):
        self.mget_responses = {
            '1:1': ['url1', 0],
            '1:2': ['url2', 0],
            '1:3': ['url3', 0],
            '1:4': ['url4', 0],
            '1:5': ['url5', 0]
        }
        self.es = MagicMock()
        self.es.mget = get_es_mget_mock(self.mget_responses)

    def _get_id_url_transformer(self, fields, es_result):
        return IdToUrlTransformer(
            fields=fields, es_result=es_result,
            es=self.es, crawl_id=CRAWL_ID, backend=ES_BACKEND
        )

    def test_harness(self):
        es_result = {
            'outlinks_internal': {
                # uid, mask
                'urls': [[5, 7]]
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

    def test_children_fields(self):
        es_result = {
            'metadata': {
                'title': {
                    'duplicates': {
                        'urls': [1, 5]
                    }
                }
            }
        }
        trans = self._get_id_url_transformer(fields=['metadata'],
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

    def test_controllable_fields(self):
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

        # partial transformation, controlled by `fields` param
        test_input = copy.deepcopy(es_result)
        trans = self._get_id_url_transformer(
            fields=['outlinks_errors.5xx'],
            es_result=[test_input]
        )
        trans.transform()

        expected = {
            'outlinks_errors': {
                '3xx': {
                    # `3xx` is not transformed
                    'urls': [1, 2, 3],
                },
                '5xx': {
                    'urls': ['url1', 'url3', 'url4', 'url5']
                }
            }
        }
        self.assertEqual(test_input, expected)

    def test_empty_result(self):
        es_result = {'other_info': 1}
        expected = copy.deepcopy(es_result)
        trans = self._get_id_url_transformer(fields=['metadata.h1'],
                                             es_result=[es_result])
        trans.transform()
        self.assertEqual(es_result, expected)

    def test_previous(self):
        es_result = {
            'previous': {
                'outlinks_internal': {
                    # uid, mask
                    'urls': [[5, 7]]
                }
            }
        }
        trans = self._get_id_url_transformer(
            fields=['previous.outlinks_internal.urls'],
            es_result=[es_result]
        )
        trans.transform()

        expected = {
            'previous': {
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
                'settings': {ES_LIST}
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

class TestHtmlExtract(unittest.TestCase):
    def test1(self):
        # q = Query('http://elasticsearch.staging.saas.botify.com:9200', 'botify_analyses',
        #           'urls', 2059, {
        #               "fields": ["url", "id", "extract.extract_i_0"],
        #               "sort": ["url"],
        #           })
        # c = q.count
        # results = list(itertools.islice(q.results, 20))
        fields = ['url', 'id', 'extract.extract_i_0']
        results = [
            {u'url': u'http://fr.ulule.com/', u'id': 1},
            {u'url': u'http://fr.ulule.com/-leap-/', u'id': -2509},
            {u'url': u'http://fr.ulule.com/1-dada-pour-2/', u'id': -2087},
            {u'url': u'http://fr.ulule.com/1083/', u'extract': {u'extract_i_0': 134},
             u'id': 454},
            {u'url': u'http://fr.ulule.com/1083/comments/', u'id': -2147},
            {u'url': u'http://fr.ulule.com/1083/news/', u'id': -2146},
            {u'url': u'http://fr.ulule.com/1083/supporters/', u'id': -2148},
            {u'url': u'http://fr.ulule.com/17ruedesarts/', u'id': 556},
            {u'url': u'http://fr.ulule.com/30ansdevih/', u'extract': {u'extract_i_0': 8},
             u'id': 1544},
        ]
        expected = [
            {u'url': u'http://fr.ulule.com/', 'extract': {'extract_i_0': None}, u'id': 1},
            {u'url': u'http://fr.ulule.com/-leap-/', 'extract': {'extract_i_0': None},
             u'id': -2509},
            {u'url': u'http://fr.ulule.com/1-dada-pour-2/',
             'extract': {'extract_i_0': None},
             u'id': -2087},
            {u'url': u'http://fr.ulule.com/1083/', u'extract': {u'extract_i_0': 134},
             u'id': 454},
            {u'url': u'http://fr.ulule.com/1083/comments/',
             'extract': {'extract_i_0': None},
             u'id': -2147},
            {u'url': u'http://fr.ulule.com/1083/news/', 'extract': {'extract_i_0': None},
             u'id': -2146}, {u'url': u'http://fr.ulule.com/1083/supporters/',
                             'extract': {'extract_i_0': None}, u'id': -2148},
            {u'url': u'http://fr.ulule.com/17ruedesarts/',
             'extract': {'extract_i_0': None},
             u'id': 556},
            {u'url': u'http://fr.ulule.com/30ansdevih/', u'extract': {u'extract_i_0': 8},
             u'id': 1544}]
        d = EmptyHtmlExtractTransformer(results, None, fields=fields)
        d.transform()
        self.assertEqual(expected, d.results)
