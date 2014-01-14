import unittest
from cdf.collections.urls.es_mapping_generation import (_parse_field_path,
                                                        generate_es_mapping,
                                                        generate_default_value_lookup)
from cdf.constants import URLS_DATA_MAPPING_DEPRECATED, URLS_DATA_FORMAT_DEFINITION


class TestMappingGeneration(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parse_field_path(self):
        path = 'a.b.c'
        result = _parse_field_path(path)
        expected = 'a.properties.b.properties.c'
        self.assertEqual(result, expected)

    def test_generation_simple(self):
        # simple case with no-index
        meta_mapping = {
            'error_links.3xx.nb': {'type': 'long'},
            'error_links.3xx.urls': {
                'type': 'long',
                'settings': {
                    'no_index',
                    'list'
                }
            }
        }

        result = generate_es_mapping(meta_mapping, routing_field=None)
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

    def test_generation_multi_field(self):
        # `multi_field` case
        meta_mapping = {
            'metadata.title': {
                'type': 'multi_field',
                'field_type': 'string',
                'settings': {
                    'list'
                }
            },
        }

        result = generate_es_mapping(meta_mapping, routing_field=None)
        expected = {
            'metadata': {
                'properties': {
                    'title': {
                        'type': 'multi_field',
                        'fields': {
                            'title': {
                                'type': 'string'
                            },
                            'untouched': {
                                'type': 'string',
                                'index': 'not_analyzed'
                            }
                        }
                    }
                }
            }
        }
        self.assertDictEqual(result['urls']['properties'], expected)

    def test_struct_field(self):
        meta_mapping = {
            'canonical_to': {
                'type': 'struct',
                'values': {
                    'url': {'type': 'string'},
                    'url_id': {'type': 'long'},
                },
                'settings': {
                    'no_index'
                }
            }
        }
        result = generate_es_mapping(meta_mapping, routing_field=None)
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

    def test_generation_all_mapping(self):
        doc_type = 'urls'
        target = URLS_DATA_MAPPING_DEPRECATED
        result = generate_es_mapping(URLS_DATA_FORMAT_DEFINITION,
                                     doc_type=doc_type)
        self.assertDictEqual(result, target)

    def test_default_value_look_up(self):
        meta_mapping = {
            'string': {'type': 'string', 'settings': {'no_index'}},
            'list': {
                'type': 'multi_field',
                'field_type': 'string',
                'settings': {
                    'list'
                }
            },
            'multi_field': {
                'type': 'multi_field',
                'field_type': 'long',
            },
            'struct_with_default': {
                'type': 'string',
                'default_value': 1,
            },
            'struct_without_default': {
                'type': 'struct',
            },
        }
        expected = {
            'string': None,
            'list': [],
            'multi_field': 0,
            'struct_with_default': 1,
            'struct_without_default': None
        }
        result = generate_default_value_lookup(meta_mapping)

        self.assertDictEqual(result, expected)