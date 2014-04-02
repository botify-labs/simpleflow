"""Stub ElasticSearch mapping for unit tests
"""

_NUMBER_TYPE = 'long'
_STRING_TYPE = 'string'
_BOOLEAN_TYPE = 'boolean'
_STRUCT_TYPE = 'struct'
_DATE_TYPE = 'date'

_NO_INDEX = 'es:no_index'
_NOT_ANALYZED = 'es:not_analyzed'
_LIST = 'list'
_MULTI_FIELD = 'es:multi_field'


STUB_FORMAT = {
    # url property data
    "url": {
        "type": _STRING_TYPE,
        "settings": {
            _NOT_ANALYZED
        }
    },
    "url_hash": {"type": _NUMBER_TYPE},
    "byte_size": {"type": _NUMBER_TYPE},
    "date_crawled": {"type": _DATE_TYPE},
    "delay1": {"type": _NUMBER_TYPE},
    "delay2": {"type": _NUMBER_TYPE},
    "depth": {"type": _NUMBER_TYPE},
    "gzipped": {"type": _BOOLEAN_TYPE},
    "content_type": {
        "type": _STRING_TYPE,
        "settings": {
            _NOT_ANALYZED
        }
    },
    "meta_noindex": {"type": _BOOLEAN_TYPE},
    "meta_nofollow": {"type": _BOOLEAN_TYPE},
    "host": {
        "type": _STRING_TYPE,
        "settings": {
            _NOT_ANALYZED
        }
    },
    "http_code": {"type": _NUMBER_TYPE},
    "id": {"type": _NUMBER_TYPE},
    "crawl_id": {"type": _NUMBER_TYPE},
    "patterns": {"type": _NUMBER_TYPE},
    "path": {
        "type": _STRING_TYPE,
        "settings": {
            _NOT_ANALYZED
        }
    },
    "protocol": {
        "type": _STRING_TYPE,
        "settings": {
            _NOT_ANALYZED
        }
    },
    "query_string": {
        "type": _STRING_TYPE,
        "settings": {
            _NOT_ANALYZED
        }
    },
    "query_string_keys": {
        "type": _STRING_TYPE,
        "settings": {
            _NOT_ANALYZED
        }
    },

    # metadata numbers
    "metadata_nb.title": {
        "type": _NUMBER_TYPE
    },
    "metadata_nb.h1": {
        "type": _NUMBER_TYPE
    },
    "metadata_nb.h2": {
        "type": _NUMBER_TYPE
    },
    "metadata_nb.description": {
        "type": _NUMBER_TYPE
    },

    # metadata contents
    "metadata.title": {
        "type": _STRING_TYPE,
        "settings": {
            _LIST,
            _MULTI_FIELD
        }
    },
    "metadata.h1": {
        "type": _STRING_TYPE,
        "settings": {
            _LIST,
            _MULTI_FIELD
        }
    },
    "metadata.h2": {
        "type": _STRING_TYPE,
        "settings": {
            _LIST,
            _MULTI_FIELD
        }
    },
    "metadata.description": {
        "type": _STRING_TYPE,
        "settings": {
            _LIST,
            _MULTI_FIELD
        }
    },

    # metadata duplication data
    "metadata_duplicate_nb.title": {"type": _NUMBER_TYPE},
    "metadata_duplicate_nb.description": {"type": _NUMBER_TYPE},
    "metadata_duplicate_nb.h1": {"type": _NUMBER_TYPE},

    "metadata_duplicate_is_first.title": {"type": _BOOLEAN_TYPE},
    "metadata_duplicate_is_first.description": {"type": _BOOLEAN_TYPE},
    "metadata_duplicate_is_first.h1": {"type": _BOOLEAN_TYPE},

    "metadata_duplicate.title": {
        "type": _NUMBER_TYPE,
        "settings": {
            _LIST,
            _NO_INDEX
        }
    },
    "metadata_duplicate.h1": {
        "type": _NUMBER_TYPE,
        "settings": {
            _LIST,
            _NO_INDEX
        }
    },
    "metadata_duplicate.description": {
        "type": _NUMBER_TYPE,
        "settings": {
            _LIST,
            _NO_INDEX
        }
    },

    # incoming links data
    "inlinks_internal_nb.total": {"type": _NUMBER_TYPE},
    "inlinks_internal_nb.follow_unique": {"type": _NUMBER_TYPE},
    "inlinks_internal_nb.total_unique": {"type": _NUMBER_TYPE},
    "inlinks_internal_nb.follow": {"type": _NUMBER_TYPE},
    "inlinks_internal_nb.nofollow": {"type": _NUMBER_TYPE},
    "inlinks_internal_nb.nofollow_combinations": {
        "type": "struct",
        "values": {
            "key": {"type": _STRING_TYPE},
            "value": {"type": _NUMBER_TYPE}
        }
    },
    "inlinks_internal": {
        "type": _NUMBER_TYPE,
        "settings": {
            _NO_INDEX,
            _LIST
        }
    },

    # outgoing links data
    # internal outgoing links
    "outlinks_internal_nb.total": {"type": _NUMBER_TYPE},
    "outlinks_internal_nb.follow_unique": {"type": _NUMBER_TYPE},
    "outlinks_internal_nb.total_unique": {"type": _NUMBER_TYPE},
    "outlinks_internal_nb.follow": {"type": _NUMBER_TYPE},
    "outlinks_internal_nb.nofollow": {"type": _NUMBER_TYPE},
    "outlinks_internal_nb.nofollow_combinations": {
        "type": "struct",
        "values": {
            "key": {"type": _STRING_TYPE},
            "value": {"type": _NUMBER_TYPE}
        }
    },
    "outlinks_internal": {
        "type": _NUMBER_TYPE,
        "settings": {
            _NO_INDEX,
            _LIST
        }
    },

    # external outgoing links
    "outlinks_external_nb.total": {"type": _NUMBER_TYPE},
    "outlinks_external_nb.follow": {"type": _NUMBER_TYPE},
    "outlinks_external_nb.nofollow": {"type": _NUMBER_TYPE},
    "outlinks_external_nb.nofollow_combinations": {
        "type": "struct",
        "values": {
            "key": {"type": _STRING_TYPE},
            "value": {"type": _NUMBER_TYPE}
        }
    },


    # incoming canonical links data
    "canonical_from_nb": {"type": _NUMBER_TYPE},
    "canonical_from": {
        "type": _NUMBER_TYPE,
        "settings": {
            _NO_INDEX,
            _LIST
        }
    },

    # outgoing canonical link data
    "canonical_to": {
        "type": "struct",
        "default_value": None,
        "values": {
            "url": {"type": _STRING_TYPE},
            "url_id": {"type": _NUMBER_TYPE},
        },
        "settings": {
            _NO_INDEX
        }
    },
    "canonical_to_equal": {"type": _BOOLEAN_TYPE},

    # outgoing redirection data
    "redirects_to": {
        "type": "struct",
        "default_value": None,
        "values": {
            "http_code": {"type": _NUMBER_TYPE},
            "url": {"type": _STRING_TYPE},
            "url_id": {"type": _NUMBER_TYPE}
        }
    },

    # incoming redirections data
    "redirects_from_nb": {"type": _NUMBER_TYPE},
    "redirects_from": {
        "type": "struct",
        "values": {
            "http_code": {"type": _NUMBER_TYPE},
            "url_id": {"type": _NUMBER_TYPE},
        },
        "settings": {
            _LIST,
            _NO_INDEX
        }
    },

    # erroneous links data
    "error_links.3xx.nb": {"type": _NUMBER_TYPE},
    "error_links.4xx.nb": {"type": _NUMBER_TYPE},
    "error_links.5xx.nb": {"type": _NUMBER_TYPE},
    "error_links.any.nb": {"type": _NUMBER_TYPE},

    "error_links.3xx.urls": {
        "type": _NUMBER_TYPE,
        "settings": {
            _NO_INDEX,
            _LIST
        }
    },
    "error_links.4xx.urls": {
        "type": _NUMBER_TYPE,
        "settings": {
            _NO_INDEX,
            _LIST
        }
    },
    "error_links.5xx.urls": {
        "type": _NUMBER_TYPE,
        "settings": {
            _NO_INDEX,
            _LIST
        }
    },
}