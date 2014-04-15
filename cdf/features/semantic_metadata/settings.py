from cdf.metadata.url.url_metadata import (
    INT_TYPE, STRING_TYPE, BOOLEAN_TYPE,
    ES_NO_INDEX, ES_NOT_ANALYZED, ES_DOC_VALUE,
    LIST, AGG_CATEGORICAL, AGG_NUMERICAL
)

__all__ = ["CONTENT_TYPE_INDEX", "CONTENT_TYPE_NAME_TO_ID",
           "MANDATORY_CONTENT_TYPES", "MANDATORY_CONTENT_TYPES_IDS"]


def _str_to_bool(string):
    return string == '1'


CONTENT_TYPE_INDEX = {
    1: 'title',
    2: 'h1',
    3: 'h2',
    4: 'description',
    5: 'h3'
}
CONTENT_TYPE_NAME_TO_ID = {v: k for k, v in CONTENT_TYPE_INDEX.iteritems()}

MANDATORY_CONTENT_TYPES = ('title', 'h1', 'description')
MANDATORY_CONTENT_TYPES_IDS = (1, 2, 4)

URLS_DATA_FORMAT_DEFINITION = {
    # title tag
    "metadata.title.nb": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL,
            AGG_CATEGORICAL
        }
    },
    "metadata.title.contents": {
        "type": STRING_TYPE,
        "settings": {ES_NOT_ANALYZED, LIST}
    },
    "metadata.title.duplicates.nb": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_CATEGORICAL,
            AGG_NUMERICAL
        }
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
        "settings": {
            ES_DOC_VALUE,
            AGG_CATEGORICAL,
            AGG_NUMERICAL
        }
    },
    "metadata.h1.contents": {
        "type": STRING_TYPE,
        "settings": {ES_NOT_ANALYZED, LIST}
    },
    "metadata.h1.duplicates.nb": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_CATEGORICAL,
            AGG_NUMERICAL
        }
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
        "settings": {
            ES_DOC_VALUE,
            AGG_CATEGORICAL,
            AGG_NUMERICAL
        }
    },
    "metadata.description.contents": {
        "type": STRING_TYPE,
        "settings": {ES_NOT_ANALYZED, LIST}
    },
    "metadata.description.duplicates.nb": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_CATEGORICAL,
            AGG_NUMERICAL
        }
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
        "settings": {
            ES_DOC_VALUE,
            AGG_CATEGORICAL,
            AGG_NUMERICAL
        }
    },
    "metadata.h2.contents": {
        "type": STRING_TYPE,
        "settings": {ES_NOT_ANALYZED, LIST}
    },

    # h3 tag
    "metadata.h3.nb": {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_CATEGORICAL,
            AGG_NUMERICAL
        }
    },
    "metadata.h3.contents": {
        "type": STRING_TYPE,
        "settings": {ES_NOT_ANALYZED, LIST}
    },
}
