from cdf.metadata.url.url_metadata import (
    INT_TYPE, BOOLEAN_TYPE, STRUCT_TYPE,
    ES_NO_INDEX, ES_DOC_VALUE,
    LIST, AGG_CATEGORICAL, AGG_NUMERICAL
)


NAME = "Link Graph"
DESCRIPTION = "Retrieve outlinks, inlinks and its status (follow, no-follow), canonicals and redirections"

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
