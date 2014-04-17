from cdf.metadata.url.url_metadata import (
    INT_TYPE, STRING_TYPE, BOOLEAN_TYPE,
    ES_NO_INDEX, ES_NOT_ANALYZED, ES_DOC_VALUE,
    LIST, AGG_CATEGORICAL, AGG_NUMERICAL
)
from cdf.features.semantic_metadata.settings import CONTENT_TYPE_INDEX
from cdf.core.streams.base import StreamDefBase


def _str_to_bool(string):
    return string == '1'


class ContentsStreamDef(StreamDefBase):
    FILE = 'urlcontents'
    HEADERS = (
        ('id', int),
        ('content_type', int),
        ('hash', int),
        ('txt', str)
    )
    URL_DOCUMENT_MAPPING = {
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

    def process_document(self, document, stream):
        content_type_id = stream[self.field_idx('content_type')]
        content_type = CONTENT_TYPE_INDEX[content_type_id]
        content = stream[self.field_idx('txt')]
        document['metadata'][content_type]['contents'].append(content)


class ContentsDuplicateStreamDef(StreamDefBase):
    FILE = 'urlcontentsduplicate'
    HEADERS = (
        ('id', int),
        ('content_type', int),
        ('filled_nb', int),
        ('duplicates_nb', int),
        ('is_first_url', _str_to_bool),
        ('duplicate_urls', lambda k: [int(i) for i in k.split(';')] if k else [])
    )
    URL_DOCUMENT_MAPPING = {
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
        "metadata.h1.duplicates.urls": {
            "type": INT_TYPE,
            "settings": {ES_NO_INDEX, LIST}
        },
        "metadata.h1.duplicates.urls_exists": {
            "type": "boolean",
            "default_value": None
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

        "metadata.description.duplicates.nb": {
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL
            }
        },
        "metadata.description.duplicates.urls": {
            "type": INT_TYPE,
            "settings": {ES_NO_INDEX, LIST}
        },
        "metadata.description.duplicates.urls_exists": {
            "type": "boolean",
            "default_value": None
        },
        "metadata.description.duplicates.is_first": {
            "type": BOOLEAN_TYPE,
        },

    }

    def process_document(self, document, stream):
        _, metadata_idx, nb_filled, nb_duplicates, is_first, duplicate_urls = stream
        metadata_type = CONTENT_TYPE_INDEX[metadata_idx]

        meta = document['metadata'][metadata_type]
        # number of metadata of this kind
        meta['nb'] = nb_filled
        # number of duplications of this piece of metadata
        dup = meta['duplicates']
        dup['nb'] = nb_duplicates
        # urls that have duplicates
        if nb_duplicates > 0:
            dup['urls'] = duplicate_urls
            dup['urls_exists'] = True

        # is this the first one out of all duplicates
        dup['is_first'] = is_first
