import unittest
from cdf.metadata.url.es_backend_utils import (_parse_field_path,
                                               ElasticSearchBackend)
from cdf.metadata.url.url_metadata import FAKE_FIELD


class TestMappingGeneration(unittest.TestCase):
    def test_parse_field_path(self):
        path = 'a.b.c'
        result = _parse_field_path(path)
        expected = 'a.properties.b.properties.c'
        self.assertEqual(result, expected)

    def test_generation_simple(self):
        # simple case with no-index
        data_format = {
            'error_links.3xx.nb': {'type': 'long'},
            'error_links.3xx.urls': {
                'type': 'long',
                'settings': {
                    'es:no_index',
                    'list'
                }
            }
        }

        es_backend = ElasticSearchBackend(data_format)
        result = es_backend.mapping(routing_field=None)
        expected = {
            'error_links': {
                'properties': {
                    '3xx': {
                        'properties': {
                            'nb': {'type': 'long'},
                            'urls': {'type': 'long', 'index': 'no'}
                        }
                    }
                }
            }
        }

        self.assertDictEqual(result['urls']['properties'], expected)

    def test_struct_field(self):
        data_format = {
            'canonical_to': {
                'type': 'struct',
                'values': {
                    'url': {'type': 'string'},
                    'url_id': {'type': 'long'},
                },
                'settings': {
                    'es:no_index'
                }
            }
        }

        es_backend = ElasticSearchBackend(data_format)
        result = es_backend.mapping(routing_field=None)
        expected = {
            'canonical_to': {
                'properties': {
                    'url': {
                        'type': 'string',
                        'index': 'no'
                    },
                    'url_id': {
                        'type': 'long',
                        'index': 'no'
                    }
                }
            }
        }
        self.assertDictEqual(result['urls']['properties'], expected)

    def test_noindex_field(self):
        data_format = {
            'field.noindex': {
                'type': 'string',
                'settings': {'es:no_index'}
            },
            'index': {'type': 'integer'}
        }
        es_backend = ElasticSearchBackend(data_format)
        result = es_backend.noindex_fields()
        expected = {'field.noindex'}
        self.assertEqual(result, expected)

    def test_doc_value(self):
        data_format = {
            'field.doc_value': {
                'type': 'string',
                'settings': {
                    'es:doc_values',
                    'es:no_index'
                }
            }
        }
        es_backend = ElasticSearchBackend(data_format)
        result = es_backend.mapping(routing_field=None)
        expectd = {
            'field': {
                'properties': {
                    'doc_value': {
                        'type': 'string',
                        'fielddata': {'format': 'doc_values'},
                        'index': 'no'
                    }
                }
            }
        }
        self.assertEqual(result['urls']['properties'], expectd)

    def test_fake_field(self):
        data_format = {
            'fake_field': {
                'settings': {
                    FAKE_FIELD
                }
            }
        }

        es_backend = ElasticSearchBackend(data_format)
        result = es_backend.mapping(routing_field=None)
        self.assertEquals(result['urls']['properties'], {})

    def test_default_value_look_up(self):
        data_format = {
            'string': {'type': 'string', 'settings': {'no_index'}},
            'list': {
                'type': 'string',
                'settings': {
                    'list',
                    'es:multi_field'
                }
            },
            'multi_field': {
                'type': 'long',
                'settings': {
                    'es:multi_field'
                }
            },
            'struct_with_default': {
                'type': 'string',
                'default_value': 1,
            },
            'struct_without_default': {
                'type': 'struct',
            },
            'no.default.value': {
                'type': 'integer',
                'default_value': None
            }
        }
        expected = {
            'string': None,
            'list': [],
            'multi_field': 0,
            'struct_with_default': 1,
            'struct_without_default': None
        }
        es_backend = ElasticSearchBackend(data_format)
        result = es_backend.field_default_value()

        self.assertDictEqual(result, expected)

    def test_query_field_lookup(self):
        data_format = {
            'error_links.3xx.urls',
            'error_links.3xx.nb',
            'error_links.4xx.urls',
            'error_links.4xx.nb',
            'one_level_field'
        }

        expected = {
            'error_links',
            'error_links.3xx',
            'error_links.4xx',
            'error_links.3xx.urls',
            'error_links.3xx.nb',
            'error_links.4xx.urls',
            'error_links.4xx.nb',
            'one_level_field'
        }
        es_backend = ElasticSearchBackend(data_format)
        result = es_backend.select_fields()

        self.assertEqual(result, expected)

    def test_agg_fields(self):
        data_format = {
            'error_links.3xx.urls': {},
            'error_links.3xx.nb': {
                'settings': {
                    'agg:numerical'
                }
            },
            'http_code': {
                'settings': {
                    'agg:numerical',
                    'agg:categorical'
                }
            }
        }

        expected = {
            'numerical': {'http_code', 'error_links.3xx.nb'},
            'categorical': {'http_code'}
        }

        es_backend = ElasticSearchBackend(data_format)
        result = es_backend.aggregation_fields()
        self.assertEqual(result, expected)

    def test_empty_document_generation(self):
        data_format = {
            'outlinks.nb.nofollow.combinations.link': {
                'type': 'integer',
                'settings': {'list'}
            },
            'outlinks.nb.nofollow.combinations.meta': {
                'type': 'long',
                'settings': {'list'}
            },
            'outlinks.nb.follow.total': {'type': 'integer'},
            'outlinks.nb.nofollow.unique': {'type': 'long'},
            'one_level_field': {'type': 'struct'}
        }
        non_flatten_expected = {
            'outlinks': {
                'nb': {
                    'nofollow': {
                        'combinations': {
                            'link': [],
                            'meta': []
                        },
                        'unique': 0,
                    },
                    'follow': {
                        'total': 0
                    },
                },
            },
            'one_level_field': None
        }

        es_backend = ElasticSearchBackend(data_format)
        result = es_backend.default_document()
        self.assertDictEqual(result, non_flatten_expected)

        flatten_expected = {
            'outlinks.nb.nofollow.combinations.link': [],
            'outlinks.nb.nofollow.combinations.meta': [],
            'outlinks.nb.follow.total': 0,
            'outlinks.nb.nofollow.unique': 0,
            'one_level_field': None
        }
        result = es_backend.default_document(flatten=True)
        self.assertDictEqual(result, flatten_expected)
