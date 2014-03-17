import unittest
from cdf.metadata.url.es_backend_utils import (_parse_field_path,
                                               ElasticSearchBackend)
from cdf.metadata.url import URLS_DATA_FORMAT_DEFINITION


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

    def test_generation_all_mapping(self):
        doc_type = 'urls'
        target = NEW_MAPPING
        es_backend = ElasticSearchBackend(URLS_DATA_FORMAT_DEFINITION)
        result = es_backend.mapping(doc_type=doc_type)

        # check individual sub-dict
        r = target['urls']['properties']
        for k, v in result['urls']['properties'].iteritems():
            self.assertEqual(v, r[k])
        # check all
        self.assertEqual(result, target)

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

_NOT_ANALYZED = "not_analyzed"
NEW_MAPPING = {
    "urls": {
        "_routing": {
            "required": True,
            "path": "crawl_id"
        },
        "_source": {
            "excludes": ["*_exists"]
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
                    "urls": {"type": "integer", "index": "no"},
                    "urls_exists": {"type": "boolean"},
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

            "outlinks_errors": {
                "properties": {
                    "3xx": {
                        "properties": {
                            "nb": {"type": "integer"},
                            "urls": {"type": "integer", "index": "no"},
                            "urls_exists": {"type": "boolean"}
                        }
                    },
                    "4xx": {
                        "properties": {
                            "nb": {"type": "integer"},
                            "urls": {"type": "integer", "index": "no"},
                            "urls_exists": {"type": "boolean"}
                        }
                    },
                    "5xx": {
                        "properties": {
                            "nb": {"type": "integer"},
                            "urls": {"type": "integer", "index": "no"},
                            "urls_exists": {"type": "boolean"}
                        }
                    },
                    "total": {"type": "integer"}
                }
            },

            "canonical": {
                "properties": {
                    "to": {
                        "properties": {
                            "url": {
                                "properties": {
                                    "url_str": {"type": "string", "index": "no"},
                                    "url_id": {"type": "integer", "index": "no"},
                                }
                            },
                            "equal": {"type": "boolean"},
                            "url_exists": {"type": "boolean"}
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

            "redirect": {
                "properties": {
                    "to": {
                        "properties": {
                            "url": {
                                "properties": {
                                    "http_code": {"type": "integer", "index": "no"},
                                    "url_str": {"type": "string", "index": "no"},
                                    "url_id": {"type": "integer", "index": "no"}
                                }
                            },
                            "url_exists": {"type": "boolean"}
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
