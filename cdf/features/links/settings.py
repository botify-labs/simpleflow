from cdf.metadata.url.url_metadata import (
    LONG_TYPE, INT_TYPE, STRING_TYPE, BOOLEAN_TYPE,
    STRUCT_TYPE, DATE_TYPE,
    ES_NO_INDEX, ES_NOT_ANALYZED, ES_DOC_VALUE,
    LIST, MULTI_FIELD,
    AGG_CATEGORICAL, AGG_NUMERICAL
)


from .helpers.masks import follow_mask

__all__ = ["STREAMS_FILES", "STREAMS_HEADERS", "URLS_DATA_FORMAT_DEFINITION"]


def _str_to_bool(string):
    return string == '1'


STREAMS_FILES = {
    'urllinks': 'outlinks',
    'urlinlinks': 'inlinks',
    'url_out_links_counters': 'outlinks_counters',
    'url_out_redirect_counters': 'outredirect_counters',
    'url_out_canonical_counters': 'outcanonical_counters',
    'url_in_links_counters': 'inlinks_counters',
    'url_in_redirect_counters': 'inredirect_counters',
    'url_in_canonical_counters': 'incanonical_counters',
    'urlbadlinks': 'badlinks',
    'urlbadlinks_counters': 'badlinks_counters'
}


STREAMS_HEADERS = {
    'OUTLINKS_RAW': (
        ('id', int),
        ('link_type', str),
        ('bitmask', int),
        ('dst_url_id', int),
        ('external_url', str)
    ),
    'INLINKS_RAW': (
        ('id', int),
        ('link_type', str),
        ('bitmask', int),
        ('src_url_id', int),
    ),
    'OUTLINKS': (
        ('id', int),
        ('link_type', str),
        ('follow', follow_mask),
        ('dst_url_id', int),
        ('external_url', str)
    ),
    'INLINKS': (
        ('id', int),
        ('link_type', str),
        ('follow', follow_mask),
        ('src_url_id', int),
    ),
    'OUTLINKS_COUNTERS': (
        ('id', int),
        ('follow', follow_mask),
        ('is_internal', _str_to_bool),
        ('score', int),
        ('score_unique', int),
    ),
    'OUTREDIRECT_COUNTERS': (
        ('id', int),
        ('is_internal', _str_to_bool)
    ),
    'OUTCANONICAL_COUNTERS': (
        ('id', int),
        ('equals', _str_to_bool)
    ),
    'INLINKS_COUNTERS': (
        ('id', int),
        ('follow', follow_mask),
        ('score', int),
        ('score_unique', int),
    ),
    'INREDIRECT_COUNTERS': (
        ('id', int),
        ('score', int)
    ),
    'INCANONICAL_COUNTERS': (
        ('id', int),
        ('score', int)
    ),
    'BADLINKS': (
        ('id', int),
        ('dst_url_id', int),
        ('http_code', int)
    ),
    'BADLINKS_COUNTERS': (
        ('id', int),
        ('http_code', int),
        ('score', int)
    )
}


URLS_DATA_FORMAT_DEFINITION = {
    # incoming links, must be internal
    "inlinks_internal.nb.total": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "inlinks_internal.nb.unique": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "inlinks_internal.nb.follow.unique": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "inlinks_internal.nb.follow.total": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "inlinks_internal.nb.nofollow.total": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "inlinks_internal.nb.nofollow.combinations.link": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "inlinks_internal.nb.nofollow.combinations.meta": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "inlinks_internal.nb.nofollow.combinations.link_meta": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
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
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_internal.nb.unique": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_internal.nb.follow.unique": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_internal.nb.follow.total": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_internal.nb.nofollow.total": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_internal.nb.nofollow.combinations.link": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_internal.nb.nofollow.combinations.meta": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_internal.nb.nofollow.combinations.robots": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_internal.nb.nofollow.combinations.link_meta": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_internal.nb.nofollow.combinations.link_robots": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_internal.nb.nofollow.combinations.meta_robots": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_internal.nb.nofollow.combinations.link_meta_robots": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
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
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_external.nb.follow.total": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_external.nb.nofollow.total": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_external.nb.nofollow.combinations.link": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_external.nb.nofollow.combinations.meta": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },
    "outlinks_external.nb.nofollow.combinations.link_meta": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    },

    # erroneous outgoing internal links
    "outlinks_errors.3xx.nb": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
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
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
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
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
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
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
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
        "settings": {
            ES_DOC_VALUE,
            AGG_CATEGORICAL,
            AGG_NUMERICAL
        }
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
        "settings": {
            ES_DOC_VALUE,
            AGG_CATEGORICAL,
            AGG_NUMERICAL
        }
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
