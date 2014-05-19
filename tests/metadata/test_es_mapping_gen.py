import unittest
from cdf.metadata.url.es_backend_utils import (_parse_field_path,
                                               ElasticSearchBackend)
from cdf.utils.features import get_urls_data_format_definition


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
            "byte_size": {
                "type": "integer",
                "fielddata": {
                    "format": "doc_values"
                }
            },
            "date_crawled": {
                "type": "date",
                "fielddata": {
                    "format": "doc_values"
                }
            },
            "delay_first_byte": {
                "type": "integer",
                "fielddata": {
                    "format": "doc_values"
                }
            },
            "delay_last_byte": {
                "type": "integer",
                "fielddata": {
                    "format": "doc_values"
                }
            },
            "depth": {
                "type": "integer",
                "fielddata": {
                    "format": "doc_values"
                }
            },
            "gzipped": {"type": "boolean"},
            "content_type": {
                "type": "string",
                "fielddata": {
                    "format": "doc_values"
                },
                "index": _NOT_ANALYZED
            },
            "host": {
                "type": "string",
                "fielddata": {
                    "format": "doc_values"
                },
                "index": _NOT_ANALYZED
            },
            "http_code": {
                "type": "integer",
                "fielddata": {
                    "format": "doc_values"
                }
            },
            "id": {
                "type": "integer",
                "fielddata": {
                    "format": "doc_values"
                }
            },
            "crawl_id": {"type": "integer"},
            "patterns": {"type": "long"},
            "path": {
                "type": "string",
                "index": _NOT_ANALYZED
            },
            "protocol": {
                "type": "string",
                "index": _NOT_ANALYZED,
                "fielddata": {
                    "format": "doc_values"
                }
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
                            "nb": {
                                "type": "integer",
                                "fielddata": {
                                    "format": "doc_values"
                                }
                            },
                            "contents": {"type": "string", "index": _NOT_ANALYZED},
                            "duplicates": {
                                "properties": {
                                    "nb": {
                                        "type": "integer",
                                        "fielddata": {
                                            "format": "doc_values"
                                        }
                                    },
                                    "is_first": {"type": "boolean"},
                                    "urls": {"type": "integer", "index": "no"},
                                    "urls_exists": {"type": "boolean"},
                                }
                            }
                        }
                    },
                    "h1": {
                        "properties": {
                            "nb": {
                                "type": "integer",
                                "fielddata": {
                                    "format": "doc_values"
                                }
                            },
                            "contents": {"type": "string", "index": _NOT_ANALYZED},
                            "duplicates": {
                                "properties": {
                                    "nb": {
                                        "type": "integer",
                                        "fielddata": {
                                            "format": "doc_values"
                                        }
                                    },
                                    "is_first": {"type": "boolean"},
                                    "urls": {"type": "integer", "index": "no"},
                                    "urls_exists": {"type": "boolean"}
                                }
                            }
                        }
                    },
                    "h2": {
                        "properties": {
                            "nb": {
                                "type": "integer",
                                "fielddata": {
                                    "format": "doc_values"
                                }
                            },
                            "contents": {"type": "string", "index": _NOT_ANALYZED},
                        }
                    },
                    "h3": {
                        "properties": {
                            "nb": {
                                "type": "integer",
                                "fielddata": {
                                    "format": "doc_values"
                                }
                            },
                            # limited to 5 contents in analysis phase
                            "contents": {"type": "string", "index": _NOT_ANALYZED},
                        }
                    },
                    "description": {
                        "properties": {
                            "nb": {
                                "type": "integer",
                                "fielddata": {
                                    "format": "doc_values"
                                }
                            },
                            "contents": {"type": "string", "index": _NOT_ANALYZED},
                            "duplicates": {
                                "properties": {
                                    "nb": {
                                        "type": "integer",
                                        "fielddata": {
                                            "format": "doc_values"
                                        }
                                    },
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
                            "total": {
                                "type": "integer",
                                "fielddata": {
                                    "format": "doc_values"
                                }
                            },
                            "unique": {
                                "type": "integer",
                                "fielddata": {
                                    "format": "doc_values"
                                }
                            },
                            "follow": {
                                "properties": {
                                    "unique": {
                                        "type": "integer",
                                        "fielddata": {
                                            "format": "doc_values"
                                        }
                                    },
                                    "total": {
                                        "type": "integer",
                                        "fielddata": {
                                            "format": "doc_values"
                                        }
                                    },
                                }
                            },
                            "nofollow": {
                                "properties": {
                                    "total": {
                                        "type": "integer",
                                        "fielddata": {
                                            "format": "doc_values"
                                        }
                                    },
                                    "combinations": {
                                        "properties": {
                                            "link": {
                                                "type": "integer",
                                                "fielddata": {
                                                    "format": "doc_values"
                                                }
                                            },
                                            "meta": {
                                                "type": "integer",
                                                "fielddata": {
                                                    "format": "doc_values"
                                                }
                                            },
                                            "link_meta": {
                                                "type": "integer",
                                                "fielddata": {
                                                    "format": "doc_values"
                                                }
                                            },
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
                            "total": {
                                "type": "integer",
                                "fielddata": {
                                    "format": "doc_values"
                                }
                            },
                            "unique": {
                                "type": "integer",
                                "fielddata": {
                                    "format": "doc_values"
                                }
                            },
                            "follow": {
                                "properties": {
                                    "unique": {
                                        "type": "integer",
                                        "fielddata": {
                                            "format": "doc_values"
                                        }
                                    },
                                    "total": {
                                        "type": "integer",
                                        "fielddata": {
                                            "format": "doc_values"
                                        }
                                    },
                                }
                            },
                            "nofollow": {
                                "properties": {
                                    "total": {
                                        "type": "integer",
                                        "fielddata": {
                                            "format": "doc_values"
                                        }
                                    },
                                    "combinations": {
                                        "properties": {
                                            "link": {
                                                "type": "integer",
                                                "fielddata": {
                                                    "format": "doc_values"
                                                }
                                            },
                                            "meta": {
                                                "type": "integer",
                                                "fielddata": {
                                                    "format": "doc_values"
                                                }
                                            },
                                            "robots": {
                                                "type": "integer",
                                                "fielddata": {
                                                    "format": "doc_values"
                                                }
                                            },
                                            "link_robots": {
                                                "type": "integer",
                                                "fielddata": {
                                                    "format": "doc_values"
                                                }
                                            },
                                            "meta_robots": {
                                                "type": "integer",
                                                "fielddata": {
                                                    "format": "doc_values"
                                                }
                                            },
                                            "link_meta": {
                                                "type": "integer",
                                                "fielddata": {
                                                    "format": "doc_values"
                                                }
                                            },
                                            "link_meta_robots": {
                                                "type": "integer",
                                                "fielddata": {
                                                    "format": "doc_values"
                                                }
                                            },
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
                            "total": {
                                "type": "integer",
                                "fielddata": {
                                    "format": "doc_values"
                                }
                            },
                            "follow": {
                                "properties": {
                                    "total": {
                                        "type": "integer",
                                        "fielddata": {
                                            "format": "doc_values"
                                        }
                                    }
                                }
                            },
                            "nofollow": {
                                "properties": {
                                    "total": {
                                        "type": "integer",
                                        "fielddata": {
                                            "format": "doc_values"
                                        }
                                    },
                                    "combinations": {
                                        "properties": {
                                            "link": {
                                                "type": "integer",
                                                "fielddata": {
                                                    "format": "doc_values"
                                                }
                                            },
                                            "meta": {
                                                "type": "integer",
                                                "fielddata": {
                                                    "format": "doc_values"
                                                }
                                            },
                                            "link_meta": {
                                                "type": "integer",
                                                "fielddata": {
                                                    "format": "doc_values"
                                                }
                                            }
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
                            "nb": {
                                "type": "integer",
                                "fielddata": {
                                    "format": "doc_values"
                                }
                            },
                            "urls": {"type": "integer", "index": "no"},
                            "urls_exists": {"type": "boolean"}
                        }
                    },
                    "4xx": {
                        "properties": {
                            "nb": {
                                "type": "integer",
                                "fielddata": {
                                    "format": "doc_values"
                                }
                            },
                            "urls": {"type": "integer", "index": "no"},
                            "urls_exists": {"type": "boolean"}
                        }
                    },
                    "5xx": {
                        "properties": {
                            "nb": {
                                "type": "integer",
                                "fielddata": {
                                    "format": "doc_values"
                                }
                            },
                            "urls": {"type": "integer", "index": "no"},
                            "urls_exists": {"type": "boolean"}
                        }
                    },
                    "total": {
                        "type": "integer",
                        "fielddata": {
                            "format": "doc_values"
                        }
                    }
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
                            "nb": {
                                "type": "integer",
                                "fielddata": {
                                    "format": "doc_values"
                                }

                            },
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
                            "nb": {
                                "type": "integer",
                                "fielddata": {
                                    "format": "doc_values"
                                }

                            },
                            "urls": {"type": "integer", "index": "no"},
                            "urls_exists": {"type": "boolean"}
                        }
                    }
                }
            },
        }
    }
}
