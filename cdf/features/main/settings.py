from cdf.metadata.url.url_metadata import (
    LONG_TYPE, INT_TYPE, STRING_TYPE, BOOLEAN_TYPE,
    STRUCT_TYPE, DATE_TYPE,
    ES_NO_INDEX, ES_NOT_ANALYZED, ES_DOC_VALUE,
    LIST, MULTI_FIELD,
    AGG_CATEGORICAL, AGG_NUMERICAL
)


__all__ = ["URLS_DATA_FORMAT_DEFINITION"]


def _str_to_bool(string):
    return string == '1'



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
            AGG_CATEGORICAL,
            AGG_NUMERICAL
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
}
