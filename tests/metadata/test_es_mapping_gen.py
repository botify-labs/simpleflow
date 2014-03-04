import unittest
from cdf.metadata.url.es_backend_utils import (_parse_field_path,
                                               generate_es_mapping,
                                               generate_default_value_lookup,
                                               generate_valid_field_lookup,
                                               generate_complete_field_lookup,
                                               generate_empty_document)
from cdf.metadata.url import URLS_DATA_FORMAT_DEFINITION


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
                    'es:no_index',
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

    @unittest.skip
    def test_generation_multi_field(self):
        # `multi_field` case
        meta_mapping = {
            'metadata.title': {
                'type': 'string',
                'settings': {
                    'list',
                    'es:multi_field'
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
                    'es:no_index'
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

    def test_generation_all_mapping_new(self):
        from cdf.metadata.url.url_metadata import URLS_DATA_FORMAT_DEFINITION_NEW
        doc_type = 'urls'
        target = NEW_MAPPING
        result = generate_es_mapping(URLS_DATA_FORMAT_DEFINITION_NEW,
                                     doc_type=doc_type)

        r = target['urls']['properties']
        for k, v in result['urls']['properties'].iteritems():
            self.assertEqual(v, r[k])

        self.assertEqual(target, result)

    def test_simple(self):
        from cdf.metadata.url.url_metadata import URLS_DATA_FORMAT_DEFINITION_NEW
        print generate_complete_field_lookup(URLS_DATA_FORMAT_DEFINITION_NEW)

    def test_default_value_look_up(self):
        meta_mapping = {
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

    def test_empty_document_generation(self):
        meta_mapping = {
            'outlinks.nb.nofollow.combinations.link': {},
            'outlinks.nb.nofollow.combinations.meta': {},
            'outlinks.nb.follow.total': {},
            'outlinks.nb.nofollow.unique': {},
            'one_level_field': {}
        }
        non_flatten_expected = {
            'outlinks': {
                'nb': {
                    'nofollow': {
                        'combinations': {
                            'link': None,
                            'meta': None
                        },
                        'unique': None,
                    },
                    'follow': {
                        'total': None
                    },
                },
            },
            'one_level_field': None
        }
        result = generate_empty_document(meta_mapping)
        self.assertDictEqual(result, non_flatten_expected)

        flatten_expected = {
            'outlinks.nb.nofollow.combinations.link': None,
            'outlinks.nb.nofollow.combinations.meta': None,
            'outlinks.nb.follow.total': None,
            'outlinks.nb.nofollow.unique': None,
            'one_level_field': None
        }
        result = generate_empty_document(meta_mapping, flatten=True)
        self.assertDictEqual(result, flatten_expected)

_NOT_ANALYZED = "not_analyzed"
URLS_DATA_MAPPING_DEPRECATED = {
    "urls": {
        "_routing": {
            "required": True,
            "path": "crawl_id"
        },
        "properties": {
            "url": {
                "type": "string",
                "index": _NOT_ANALYZED
            },
            "url_hash": {"type": "long"},
            "byte_size": {"type": "long"},
            "date_crawled": {"type": "date"},
            "delay1": {"type": "long"},
            "delay2": {"type": "long"},
            "depth": {"type": "long"},
            "gzipped": {"type": "boolean"},
            "content_type": {
                "type": "string",
                "index": _NOT_ANALYZED
            },
            "meta_noindex": {"type": "boolean"},
            "host": {
                "type": "string",
                "index": _NOT_ANALYZED
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
                        "type": "string",
                        "index": _NOT_ANALYZED
                    },
                    "h1": {
                        "type": "string",
                        "index": _NOT_ANALYZED
                    },
                    "h2": {
                        "type": "string",
                        "index": _NOT_ANALYZED
                    },
                    "title": {
                        "type": "string",
                        "index": _NOT_ANALYZED
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
                "index": _NOT_ANALYZED
            },
            "protocol": {
                "type": "string",
                "index": _NOT_ANALYZED
            },
            "query_string": {
                "type": "string",
                "index": _NOT_ANALYZED
            },
            "query_string_keys": {
                "type": "string",
                "index": _NOT_ANALYZED
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

NEW_MAPPING = {
    "urls": {
        "_routing": {
            "required": True,
            "path": "crawl_id"
        },
        "properties": {
            # simple properties for each url
            "url": {
                "type": "string",
                "index": _NOT_ANALYZED
            },
            "url_hash": {"type": "long"},
            "byte_size": {"type": "integer"},
            "date_crawled": {"type": "date"},
            "delay_first_byte": {"type": "integer"},
            "delay_last_byte": {"type": "integer"},
            "depth": {"type": "integer"},
            "gzipped": {"type": "boolean"},
            "content_type": {
                "type": "string",
                "index": _NOT_ANALYZED
            },
            "host": {
                "type": "string",
                "index": _NOT_ANALYZED
            },
            "http_code": {"type": "integer"},
            "id": {"type": "integer"},
            "crawl_id": {"type": "integer"},
            "patterns": {"type": "long"},
            "path": {
                "type": "string",
                "index": _NOT_ANALYZED
            },
            "protocol": {
                "type": "string",
                "index": _NOT_ANALYZED
            },
            "query_string": {
                "type": "string",
                "index": _NOT_ANALYZED
            },
            "query_string_keys": {
                "type": "string",
                "index": _NOT_ANALYZED
            },

            # metadata related properties
            "metadata": {
                "properties": {
                    "robots": {
                        "properties": {
                            "nofollow": {"type": "boolean"},
                            "noindex": {"type": "boolean"},
                        }
                    },

                    "title": {
                        "properties": {
                            "nb": {"type": "integer"},
                            "contents": {"type": "string", "index": _NOT_ANALYZED},
                            "duplicates": {
                                "properties": {
                                    "nb": {"type": "integer"},
                                    "is_first": {"type": "boolean"},
                                    "urls": {"type": "integer", "index": "no"},
                                    "urls_exists": {"type": "boolean"},
                                }
                            }
                        }
                    },
                    "h1": {
                        "properties": {
                            "nb": {"type": "integer"},
                            "contents": {"type": "string", "index": _NOT_ANALYZED},
                            "duplicates": {
                                "properties": {
                                    "nb": {"type": "integer"},
                                    "is_first": {"type": "boolean"},
                                    "urls": {"type": "integer", "index": "no"},
                                    "urls_exists": {"type": "boolean"}
                                }
                            }
                        }
                    },
                    "h2": {
                        "properties": {
                            "nb": {"type": "integer"},
                            "contents": {"type": "string", "index": _NOT_ANALYZED},
                        }
                    },
                    "h3": {
                        "properties": {
                            "nb": {"type": "integer"},
                            # limited to 5 contents in analysis phase
                            "contents": {"type": "string", "index": _NOT_ANALYZED},
                        }
                    },
                    "description": {
                        "properties": {
                            "nb": {"type": "integer"},
                            "contents": {"type": "string", "index": _NOT_ANALYZED},
                            "duplicates": {
                                "properties": {
                                    "nb": {"type": "integer"},
                                    "is_first": {"type": "boolean"},
                                    "urls": {"type": "integer", "index": "no"},
                                    "urls_exists": {"type": "boolean"}
                                }
                            }
                        }
                    },
                }
            },

            "inlinks_internal": {
                "properties": {
                    "nb": {
                        "properties": {
                            "total": {"type": "integer"},
                            "unique": {"type": "integer"},
                            "follow": {
                                "properties": {
                                    "unique": {"type": "integer"},
                                    "total": {"type": "integer"},
                                }
                            },
                            "nofollow": {
                                "properties": {
                                    "total": {"type": "integer"},
                                    "combinations": {
                                        "properties": {
                                            "link": {"type": "integer"},
                                            "meta": {"type": "integer"},
                                            "link_meta": {"type": "integer"},
                                        }
                                    }
                                }
                            },
                        }
                    },
                    # list of source urls of incoming links
                    # truncated at 300 urls
                    "urls": {"type": "integer", "index": "no"},
                    "urls_exists": {"type": "boolean"}
                }
            },

            "outlinks_internal": {
                "properties": {
                    "nb": {
                        "properties": {
                            "errors": {
                                "properties": {
                                    "total": {"type": "integer"},
                                    "3xx": {"type": "integer"},
                                    "4xx": {"type": "integer"},
                                    "5xx": {"type": "integer"},
                                }
                            },
                            "total": {"type": "integer"},
                            "unique": {"type": "integer"},
                            "follow": {
                                "properties": {
                                    "unique": {"type": "integer"},
                                    "total": {"type": "integer"},
                                }
                            },
                            "nofollow": {
                                "properties": {
                                    "total": {"type": "integer"},
                                    "combinations": {
                                        "properties": {
                                            "link": {"type": "integer"},
                                            "meta": {"type": "integer"},
                                            "robots": {"type": "integer"},
                                            "link_robots": {"type": "integer"},
                                            "meta_robots": {"type": "integer"},
                                            "link_meta": {"type": "integer"},
                                            "link_meta_robots": {"type": "integer"},
                                        }
                                    }
                                }
                            },
                        }
                    },
                    "urls": {
                        "properties": {
                            "all": {"type": "integer", "index": "no"},
                            "3xx": {"type": "integer", "index": "no"},
                            "4xx": {"type": "integer", "index": "no"},
                            "5xx": {"type": "integer", "index": "no"},

                            "all_exists": {"type": "boolean"},
                            "3xx_exists": {"type": "boolean"},
                            "4xx_exists": {"type": "boolean"},
                            "5xx_exists": {"type": "boolean"},
                        }
                    },
                }
            },

            "outlinks_external": {
                "properties": {
                    "nb": {
                        "properties": {
                            "total": {"type": "integer"},
                            "follow": {
                                "properties": {
                                    "total": {"type": "integer"}
                                }
                            },
                            "nofollow": {
                                "properties": {
                                    "total": {"type": "integer"},
                                    "combinations": {
                                        "properties": {
                                            "link": {"type": "integer"},
                                            "meta": {"type": "integer"},
                                            "link_meta": {"type": "integer"}
                                        }
                                    }
                                }
                            },
                        }
                    }
                }
            },

            "canonical": {
                "properties": {
                    "to": {
                        "properties": {
                            "url": {"type": "string", "index": _NOT_ANALYZED},
                            "url_id": {"type": "integer"},
                            "equal": {"type": "boolean"},
                        }
                    },

                    "from": {
                        "properties": {
                            "nb": {"type": "integer"},
                            "urls": {"type": "integer", "index": "no"},
                            "urls_exists" : {"type": "boolean"}
                        }
                    }
                }
            },

            "redirect": {
                "properties": {
                    "to": {
                        "properties": {
                            "http_code": {"type": "integer"},
                            "url": {"type": "string", "index": _NOT_ANALYZED},
                            "url_id": {"type": "integer"}
                        }
                    },
                    "from": {
                        "properties": {
                            "nb": {"type": "integer"},
                            "urls": {"type": "integer", "index": "no"},
                            "urls_exists": {"type": "boolean"}
                        }
                    }
                }
            },
        }
    }
}