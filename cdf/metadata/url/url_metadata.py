# A intermediate definition of url data format
#
# Keys are represented in a path format
#   - ex. `metadata.h1`
#       This means `metadata` will be an object type and it
#       contains a field named `h1`
#
# Values contains
#   - type: data type of this field
#       - long: for large numeric values, such as hash value
#       - integer: for numeric values
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
#       - es:multi_field: a multi_field type keeps multiple copies of the same
#           data in different format (analyzed, not_analyzed etc.)
#           In case of `multi_field`, `field_type` must be specified for
#           determine the field's type
#       - list: this field is actually a list of values in ES
#
#   - default_value (optional): the default value if this field does not
#       exist. If this key is not present, the field's default value will be
#       inferred based on its type
#       Set to `None` to avoid any default values, so if this field is missing
#       in ElasticSearch result, no default value will be added

# Type related
LONG_TYPE = 'long'
INT_TYPE = 'integer'
STRING_TYPE = 'string'
BOOLEAN_TYPE = 'boolean'
STRUCT_TYPE = 'struct'
DATE_TYPE = 'date'

# Data format related
ES_NO_INDEX = 'es:no_index'
ES_NOT_ANALYZED = 'es:not_analyzed'
ES_DOC_VALUE = 'es:doc_values'
LIST = 'list'
MULTI_FIELD = 'es:multi_field'

# Aggregation related
# categorical fields have a finite cardinality of distinct values
AGG_CATEGORICAL = 'agg:categorical'
AGG_NUMERICAL = 'agg:numerical'


URLS_DATA_FORMAT_DEFINITION = {
    # url property data
    "url": {
        "type": STRING_TYPE,
        "settings": {ES_NOT_ANALYZED}
    },
    "url_hash": {"type": LONG_TYPE},
    "byte_size": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "date_crawled": {
        "type": DATE_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "delay_first_byte": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "delay_last_byte": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "depth": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            # assume possible depth is finite
            AGG_CATEGORICAL
        }
    },
    "gzipped": {"type": BOOLEAN_TYPE},
    "content_type": {
        "type": STRING_TYPE,
        "settings": {
            ES_NOT_ANALYZED,
            ES_DOC_VALUE,
            AGG_CATEGORICAL
        }
    },
    "host": {
        "type": STRING_TYPE,
        "settings": {
            ES_NOT_ANALYZED,
            ES_DOC_VALUE,
            AGG_CATEGORICAL
        }
    },
    "http_code": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            # `http_code` have 2 roles
            AGG_CATEGORICAL,
            AGG_NUMERICAL
        }
    },
    "id": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "crawl_id": {"type": INT_TYPE},
    "patterns": {
        "type": LONG_TYPE,
        "settings": {
            LIST
        }
    },
    "path": {
        "type": STRING_TYPE,
        "settings": {ES_NOT_ANALYZED}
    },
    "protocol": {
        "type": STRING_TYPE,
        "settings": {
            ES_NOT_ANALYZED,
            ES_DOC_VALUE,
            AGG_CATEGORICAL
        }
    },
    "query_string": {
        "type": STRING_TYPE,
        "settings": {ES_NOT_ANALYZED}
    },
    "query_string_keys": {
        "type": STRING_TYPE,
        "settings": {ES_NOT_ANALYZED}
    },

    # meta tag related
    "metadata.robots.nofollow": {
        "type": BOOLEAN_TYPE,
        "settings": {AGG_CATEGORICAL}
    },
    "metadata.robots.noindex": {
        "type": BOOLEAN_TYPE,
        "settings": {AGG_CATEGORICAL}
    },

    # title tag
    "metadata.title.nb": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "metadata.title.contents": {
        "type": STRING_TYPE,
        "settings": {ES_NOT_ANALYZED, LIST}
    },
    "metadata.title.duplicates.nb": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "metadata.title.duplicates.is_first": {
        "type": BOOLEAN_TYPE,
    },
    "metadata.title.duplicates.urls": {
        "type": INT_TYPE,
        "settings": {ES_NO_INDEX, LIST}
    },
    "metadata.title.duplicates.urls_exists": {
        "type": "boolean",
        "default_value": None
    },

    # h1 tag
    "metadata.h1.nb": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "metadata.h1.contents": {
        "type": STRING_TYPE,
        "settings": {ES_NOT_ANALYZED, LIST}
    },
    "metadata.h1.duplicates.nb": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "metadata.h1.duplicates.is_first": {
        "type": BOOLEAN_TYPE,
    },
    "metadata.h1.duplicates.urls": {
        "type": INT_TYPE,
        "settings": {ES_NO_INDEX, LIST}
    },
    "metadata.h1.duplicates.urls_exists": {
        "type": "boolean",
        "default_value": None
    },

    # description tag
    "metadata.description.nb": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "metadata.description.contents": {
        "type": STRING_TYPE,
        "settings": {ES_NOT_ANALYZED, LIST}
    },
    "metadata.description.duplicates.nb": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "metadata.description.duplicates.is_first": {
        "type": BOOLEAN_TYPE,
    },
    "metadata.description.duplicates.urls": {
        "type": INT_TYPE,
        "settings": {ES_NO_INDEX, LIST}
    },
    "metadata.description.duplicates.urls_exists": {
        "type": "boolean",
        "default_value": None
    },

    # h2 tag
    "metadata.h2.nb": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "metadata.h2.contents": {
        "type": STRING_TYPE,
        "settings": {ES_NOT_ANALYZED, LIST}
    },

    # h3 tag
    "metadata.h3.nb": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "metadata.h3.contents": {
        "type": STRING_TYPE,
        "settings": {ES_NOT_ANALYZED, LIST}
    },

    # incoming links, must be internal
    "inlinks_internal.nb.total": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "inlinks_internal.nb.unique": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "inlinks_internal.nb.follow.unique": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "inlinks_internal.nb.follow.total": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "inlinks_internal.nb.nofollow.total": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "inlinks_internal.nb.nofollow.combinations.link": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "inlinks_internal.nb.nofollow.combinations.meta": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "inlinks_internal.nb.nofollow.combinations.link_meta": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "inlinks_internal.urls": {
        "type": INT_TYPE,
        "settings": {ES_NO_INDEX, LIST}
    },
    "inlinks_internal.urls_exists": {
        "type": "boolean",
        "default_value": None
    },

    # internal outgoing links (destination is a internal url)
    "outlinks_internal.nb.total": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_internal.nb.unique": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_internal.nb.follow.unique": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_internal.nb.follow.total": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_internal.nb.nofollow.total": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_internal.nb.nofollow.combinations.link": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_internal.nb.nofollow.combinations.meta": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_internal.nb.nofollow.combinations.robots": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_internal.nb.nofollow.combinations.link_meta": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_internal.nb.nofollow.combinations.link_robots": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_internal.nb.nofollow.combinations.meta_robots": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_internal.nb.nofollow.combinations.link_meta_robots": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_internal.urls": {
        "type": INT_TYPE,
        "settings": {ES_NO_INDEX, LIST},
    },
    "outlinks_internal.urls_exists": {
        "type": BOOLEAN_TYPE,
        "default_value": None
    },

    # external outgoing links (destination is a external url)
    "outlinks_external.nb.total": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_external.nb.follow.total": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_external.nb.nofollow.total": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_external.nb.nofollow.combinations.link": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_external.nb.nofollow.combinations.meta": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_external.nb.nofollow.combinations.link_meta": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },

    # erroneous outgoing internal links
    "outlinks_errors.3xx.nb": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_errors.3xx.urls": {
        "type": INT_TYPE,
        "settings": {ES_NO_INDEX, LIST}
    },
    "outlinks_errors.3xx.urls_exists": {
        "type": "boolean",
        "default_value": None
    },

    "outlinks_errors.4xx.nb": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_errors.4xx.urls": {
        "type": INT_TYPE,
        "settings": {ES_NO_INDEX, LIST}
    },
    "outlinks_errors.4xx.urls_exists": {
        "type": "boolean",
        "default_value": None
    },

    "outlinks_errors.5xx.nb": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "outlinks_errors.5xx.urls": {
        "type": INT_TYPE,
        "settings": {ES_NO_INDEX, LIST}
    },
    "outlinks_errors.5xx.urls_exists": {
        "type": "boolean",
        "default_value": None
    },
    # total error_links number
    "outlinks_errors.total": {
        "type": "integer",
        "settings": {ES_DOC_VALUE}
    },

    # outgoing canonical link, one per page
    # if multiple, first one is taken into account
    "canonical.to.url": {
        "type": STRUCT_TYPE,
        "values": {
            "url_str": {"type": "string"},
            "url_id": {"type": "integer"},
        },
        "settings": {
            ES_NO_INDEX
        }
    },
    "canonical.to.equal": {
        "type": BOOLEAN_TYPE,
        "settings": {AGG_CATEGORICAL}
    },
    "canonical.to.url_exists": {
        "type": "boolean",
        "default_value": None
    },

    # incoming canonical link
    "canonical.from.nb": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "canonical.from.urls": {
        "type": INT_TYPE,
        "settings": {ES_NO_INDEX, LIST}
    },
    "canonical.from.urls_exists": {
        "type": "boolean",
        "default_value": None
    },

    # outgoing redirection
    "redirect.to.url": {
        "type": STRUCT_TYPE,
        "values": {
            "url_str": {"type": "string"},
            "url_id": {"type": "integer"},
            "http_code": {"type": "integer"}
        },
        "settings": {
            ES_NO_INDEX
        }
    },
    "redirect.to.url_exists": {
        "type": BOOLEAN_TYPE,
        "default_value": None
    },

    # incoming redirection
    "redirect.from.nb": {
        "type": INT_TYPE,
        "settings": {ES_DOC_VALUE}
    },
    "redirect.from.urls": {
        "type": INT_TYPE,
        "settings": {ES_NO_INDEX, LIST}
    },
    "redirect.from.urls_exists": {
        "type": "boolean",
        "default_value": None
    },
}
