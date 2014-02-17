from cdf.streams.mapping import CONTENT_TYPE_NAME_TO_ID


SUGGEST_CLUSTERS = ['mixed']


CLUSTER_TYPE_TO_ID = {
    'pattern': {
        'host': 10,
        'path': 11,
        'qskey': 12,
    },
    'metadata': {
        CONTENT_TYPE_NAME_TO_ID['title']: 20,
        CONTENT_TYPE_NAME_TO_ID['description']: 21,
        CONTENT_TYPE_NAME_TO_ID['h1']: 22,
        CONTENT_TYPE_NAME_TO_ID['h2']: 23,
        CONTENT_TYPE_NAME_TO_ID['h3']: 32
    }
}


# A intermediate definition of url data format
#
# Keys are represented in a path format
#   - ex. `metadata.h1`
#       This means `metadata` will be an object type and it
#       contains a field named `h1`
#
# Values contains
#   - type: data type of this field
#       - long: for numeric values
#       - string: for string values
#       - struct: struct can contains some inner fields, but these fields
#           won't be visible when querying
#           ex. `something.redirects_from:
#               [{`id`: xx, `http_code`: xx}, {...}, ...]`
#               `redirects_from` is visible, but `redirects_from.id` is not
#           Struct's inner fields have their own types
#
#   - settings (optional): a set of setting flags of this field
#       - es:not_analyzed: this field should not be tokenized by ES
#       - es:no_index: this field should not be indexed
#       - list: this field is actually a list of values in ES
#       - es:multi_field: a multi_field type keeps multiple copies of the same
#           data in different format (analyzed, not_analyzed etc.)
#           In case of `multi_field`, `field_type` must be specified for
#           determine the field's type

#
#   - default_value (optional): the default value if this field does not
#       exist

_NUMBER_TYPE = 'long'
_STRING_TYPE = 'string'
_BOOLEAN_TYPE = 'boolean'
_STRUCT_TYPE = 'struct'
_DATE_TYPE = 'date'

_NO_INDEX = 'es:no_index'
_NOT_ANALYZED = 'es:not_analyzed'
_LIST = 'list'
_MULTI_FIELD = 'es:multi_field'


URLS_DATA_FORMAT_DEFINITION = {
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
    "content_type": {"type": _STRING_TYPE},
    "meta_no_index": {"type": _BOOLEAN_TYPE},
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
