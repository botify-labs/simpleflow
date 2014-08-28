from cdf.metadata.url.url_metadata import (
    INT_TYPE, STRING_TYPE, BOOLEAN_TYPE,
    ES_NO_INDEX, ES_NOT_ANALYZED, ES_DOC_VALUE,
    LIST, AGG_CATEGORICAL, AGG_NUMERICAL, URL_ID
)
from cdf.features.semantic_metadata.settings import CONTENT_TYPE_INDEX
from cdf.core.streams.base import StreamDefBase
from cdf.query.constants import RENDERING, FIELD_RIGHTS


def _raw_to_bool(string):
    return string == '1'


class ContentsStreamDef(StreamDefBase):
    FILE = 'urlcontents'
    HEADERS = (
        ('id', int),
        ('content_type', int),
        ('hash', int),
        ('txt', str)
    )
    URL_DOCUMENT_DEFAULT_GROUP = "semantic_metadata"
    URL_DOCUMENT_MAPPING = {
        # title tag
        "metadata.title.contents": {
            "verbose_name": "Title",
            "order": 1,
            "type": STRING_TYPE,
            "settings": {ES_NOT_ANALYZED, LIST}
        },
        # h1 tag
        "metadata.h1.contents": {
            "verbose_name": "H1",
            "order": 3,
            "type": STRING_TYPE,
            "settings": {ES_NOT_ANALYZED, LIST}
        },
        # description tag
        "metadata.description.contents": {
            "verbose_name": "Page description",
            "type": STRING_TYPE,
            "order": 2,
            "settings": {ES_NOT_ANALYZED, LIST}
        },
        # h2 tag
        "metadata.h2.contents": {
            "verbose_name": "H2",
            "order": 4,
            "type": STRING_TYPE,
            "settings": {ES_NOT_ANALYZED, LIST}
        },

        # h3 tag
        "metadata.h3.contents": {
            "verbose_name": "H3",
            "type": STRING_TYPE,
            "order": 5,
            "settings": {ES_NOT_ANALYZED, LIST}
        },
    }

    def process_document(self, document, stream):
        content_type_id = stream[self.field_idx('content_type')]
        content_type = CONTENT_TYPE_INDEX[content_type_id]
        content = stream[self.field_idx('txt')]
        document['metadata'][content_type]['contents'].append(content)


def _get_duplicate_document_mapping(duplicate_type, verbose_duplicate_type):
    """Generate mapping for duplicate documents
    :param duplicate_type: the kind of duplicate.
                           this string will be used to generate the field types
    :type verbose_duplicate_type: the description of the duplicate type.
                                  this string will be used to generate the
                                  verbose names.
    :returns: dict
    """
    result = {}
    for i, metadata_type in enumerate(["title", "h1", "description"]):
        prefix = "metadata.{}.{}".format(metadata_type, duplicate_type)
        result["{}.nb".format(prefix)] = {
            "verbose_name": "Number of {} {}".format(
                verbose_duplicate_type, metadata_type.capitalize()
            ),
            "type": INT_TYPE,
            "order": 100 + i,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL
            }
        }
        result["{}.is_first".format(prefix)] = {
            "verbose_name": "First {} {} found".format(
                verbose_duplicate_type, metadata_type.capitalize()
            ),
            "order": 120 + i,
            "type": BOOLEAN_TYPE,
        }
        result["{}.urls".format(prefix)] = {
            "verbose_name": "Pages with the same {}".format(metadata_type.capitalize()),
            "type": INT_TYPE,
            "order": 110 + 1,
            "settings": {
                ES_NO_INDEX,
                LIST,
                RENDERING.URL_STATUS,
                URL_ID
            }
        }

        result["{}.urls_exists".format(prefix)] = {
            "type": "boolean",
            "default_value": None
        }
    return result


class ContentsDuplicateStreamDef(StreamDefBase):
    FILE = 'urlcontentsduplicate'
    HEADERS = (
        ('id', int),
        ('content_type', int),
        ('duplicates_nb', int),
        ('is_first_url', _raw_to_bool),
        ('duplicate_urls', lambda k: [int(i) for i in k.split(';')] if k else [])
    )
    URL_DOCUMENT_DEFAULT_GROUP = "semantic_metadata"

    URL_DOCUMENT_MAPPING = _get_duplicate_document_mapping(
        "duplicates",
        "duplicate"
    )

    def process_document(self, document, stream):
        _, metadata_idx, nb_duplicates, is_first, duplicate_urls = stream
        metadata_type = CONTENT_TYPE_INDEX[metadata_idx]

        meta = document['metadata'][metadata_type]
        # number of duplications of this piece of metadata
        dup = meta['duplicates']
        dup['nb'] = nb_duplicates
        # urls that have duplicates
        if nb_duplicates > 0:
            dup['urls'] = duplicate_urls
            dup['urls_exists'] = True

        # is this the first one out of all duplicates
        dup['is_first'] = is_first


class ContentsCountStreamDef(StreamDefBase):
    FILE = 'urlcontents_count'
    HEADERS = (
        ('id', int),
        ('content_type', int),
        ('filled_nb', int),
    )
    URL_DOCUMENT_DEFAULT_GROUP = "semantic_metadata"
    URL_DOCUMENT_MAPPING = {
        # title tag
        "metadata.title.nb": {
            "verbose_name": "Number of Page Titles",
            "type": INT_TYPE,
            "order": 10,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                AGG_CATEGORICAL
            }
        },
        # h1 tag
        "metadata.h1.nb": {
            "verbose_name": "Number of H1",
            "type": INT_TYPE,
            "order": 12,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL
            }
        },
        # description tag
        "metadata.description.nb": {
            "verbose_name": "Number of Page Description",
            "type": INT_TYPE,
            "order": 11,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL
            }
        },
        # h2 tag
        "metadata.h2.nb": {
            "verbose_name": "Number of H2",
            "type": INT_TYPE,
            "order": 13,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL
            }
        },
        # h3 tag
        "metadata.h3.nb": {
            "verbose_name": "Number of H3",
            "type": INT_TYPE,
            "order": 14,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL
            }
        }
    }

    def process_document(self, document, stream):
        _, metadata_idx, nb_filled = stream
        metadata_type = CONTENT_TYPE_INDEX[metadata_idx]
        document['metadata'][metadata_type]['nb'] = nb_filled
