import unittest
from cdf.collections.urls.es_mapping_generation import (_parse_field_path,
                                                        generate_es_mapping,
                                                        generate_default_value_lookup,
                                                        generate_valid_field_lookup)
from cdf.collections.urls.constants import URLS_DATA_FORMAT_DEFINITION


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
                'type': 'string',
                'settings': {
                    'list',
                    'multi_field'
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
                'type': 'long',
                'settings': {
                    'multi_field'
                }
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

    def test_valid_field_lookup(self):
        meta_mapping = {
            'error_links.3xx.urls',
            'error_links.3xx.nb',
            'error_links.4xx.urls',
            'error_links.4xx.nb',
            'one_level_field'
        }

        result = generate_valid_field_lookup(meta_mapping)
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

        self.assertEqual(result, expected)

URLS_DATA_MAPPING_DEPRECATED = {
    "urls": {
        "_routing": {
            "required": True,
            "path": "crawl_id"
        },
        "properties": {
            "url": {
                "type": "string",
                "index": "not_analyzed"
            },
            "url_hash": {"type": "long"},
            "byte_size": {"type": "long"},
            "date_crawled": {"type": "date"},
            "delay1": {"type": "long"},
            "delay2": {"type": "long"},
            "depth": {"type": "long"},
            "gzipped": {"type": "boolean"},
            "host": {
                "type": "string",
                "index": "not_analyzed"
            },
            "http_code": {"type": "long"},
            "id": {"type": "long"},
            "crawl_id": {"type": "long"},
            "patterns": {"type": "long"},
            "metadata_nb": {
                "properties": {
                    "description": {"type": "long"},
                    "h1": {"type": "long"},
                    "h2": {"type": "long"},
                    "title": {"type": "long"}
                }
            },
            "metadata": {
                "properties": {
                    "description": {
                        "type": "multi_field",
                        "fields": {
                            "description": {
                                "type": "string"
                            },
                            "untouched": {
                                "type": "string",
                                "index": "not_analyzed"
                            }
                        }
                    },
                    "h1": {
                        "type": "multi_field",
                        "fields": {
                            "h1": {
                                "type": "string"
                            },
                            "untouched": {
                                "type": "string",
                                "index": "not_analyzed"
                            }
                        }
                    },
                    "h2": {
                        "type": "multi_field",
                        "fields": {
                            "h2": {
                                "type": "string"
                            },
                            "untouched": {
                                "type": "string",
                                "index": "not_analyzed"
                            }
                        }
                    },
                    "title": {
                        "type": "multi_field",
                        "fields": {
                            "title": {
                                "type": "string"
                            },
                            "untouched": {
                                "type": "string",
                                "index": "not_analyzed"
                            }
                        }
                    }
                }
            },
            "metadata_duplicate_nb": {
                "properties": {
                    "title": {"type": "long"},
                    "description": {"type": "long"},
                    "h1": {"type": "long"}
                }
            },
            "metadata_duplicate": {
                "properties": {
                    "title": {"type": "long", "index": "no"},
                    "description": {"type": "long", "index": "no"},
                    "h1": {"type": "long", "index": "no"}
                }
            },
            "metadata_duplicate_is_first": {
                "properties": {
                    "title": {"type": "boolean"},
                    "description": {"type": "boolean"},
                    "h1": {"type": "boolean"}
                }
            },
            "inlinks_internal_nb": {
                "properties": {
                    "total": {"type": "long"},
                    "follow_unique": {"type": "long"},
                    "total_unique": {"type": "long"},
                    "follow": {"type": "long"},
                    "nofollow": {"type": "long"},
                    "nofollow_combinations": {
                        "properties": {
                            "key": {"type": "string"},
                            "value": {"type": "long"}
                        }
                    }
                }
            },
            "inlinks_internal": {"type": "long", "index": "no"},
            "outlinks_internal": {"type": "long", "index": "no"},
            "outlinks_internal_nb": {
                "properties": {
                    "total": {"type": "long"},
                    "follow_unique": {"type": "long"},
                    "total_unique": {"type": "long"},
                    "follow": {"type": "long"},
                    "nofollow": {"type": "long"},
                    "nofollow_combinations": {
                        "properties": {
                            "key": {"type": "string"},
                            "value": {"type": "long"}
                        }
                    }
                }
            },
            "outlinks_external_nb": {
                "properties": {
                    "total": {"type": "long"},
                    "follow": {"type": "long"},
                    "nofollow": {"type": "long"},
                    "nofollow_combinations": {
                        "properties": {
                            "key": {"type": "string"},
                            "value": {"type": "long"}
                        }
                    }
                }
            },
            "path": {
                "type": "string",
                "index": "not_analyzed"
            },
            "protocol": {
                "type": "string",
                "index": "not_analyzed"
            },
            "query_string": {
                "type": "string",
                "index": "not_analyzed"
            },
            "query_string_keys": {
                "type": "string",
                "index": "not_analyzed"
            },
            "canonical_from_nb": {"type": "long"},
            "canonical_from": {"type": "long", "index": "no"},
            "canonical_to": {
                "properties": {
                    "url": {"type": "string", "index": "no"},
                    "url_id": {"type": "long", "index": "no"}
                }
            },
            "canonical_to_equal": {"type": "boolean"},
            "redirects_to": {
                "properties": {
                    "http_code": {"type": "long"},
                    "url": {"type": "string"},
                    "url_id": {"type": "long"}
                }
            },
            "redirects_from_nb": {"type": "long"},
            "redirects_from": {
                "properties": {
                    "http_code": {"type": "long", "index": "no"},
                    "url_id": {"type": "long", "index": "no"}
                }
            },
            "error_links": {
                "properties": {
                    "3xx": {
                        "properties": {
                            "nb": {"type": "long"},
                            "urls": {"type": "long", "index": "no"}
                        }
                    },
                    "4xx": {
                        "properties": {
                            "nb": {"type": "long"},
                            "urls": {"type": "long", "index": "no"}
                        }
                    },
                    "5xx": {
                        "properties": {
                            "nb": {"type": "long"},
                            "urls": {"type": "long", "index": "no"}
                        }
                    },
                    "any": {
                        "properties": {
                            "nb": {"type": "long"}
                        }
                    }
                }
            }
        }
    }
}
