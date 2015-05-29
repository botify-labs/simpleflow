from cdf.core.metadata.dataformat import check_enabled
from cdf.utils.dict import get_subdict_from_path
from cdf.metadata.url.url_metadata import (
    INT_TYPE, STRING_TYPE, BOOLEAN_TYPE,
    ES_NO_INDEX, ES_NOT_ANALYZED, ES_DOC_VALUE,
    ES_LIST, AGG_CATEGORICAL, AGG_NUMERICAL, URL_ID,
    DIFF_QUALITATIVE, DIFF_QUANTITATIVE
)
from cdf.features.semantic_metadata.settings import CONTENT_TYPE_INDEX
from cdf.core.streams.base import StreamDefBase
from cdf.core.metadata.constants import RENDERING, FIELD_RIGHTS


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
        "metadata.title.nb": {
            "verbose_name": "No. of Titles",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                AGG_CATEGORICAL,
                DIFF_QUANTITATIVE
            }
        },
        "metadata.title.contents": {
            "verbose_name": "Title",
            "type": STRING_TYPE,
            "settings": {
                ES_NOT_ANALYZED,
                ES_LIST,
                DIFF_QUALITATIVE
            }
        },
        # h1 tag
        "metadata.h1.nb": {
            "verbose_name": "No. of H1",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "metadata.h1.contents": {
            "verbose_name": "H1",
            "type": STRING_TYPE,
            "settings": {
                ES_NOT_ANALYZED,
                ES_LIST,
                DIFF_QUALITATIVE
            }
        },
        # description tag
        "metadata.description.nb": {
            "verbose_name": "No. of Meta Descriptions",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "metadata.description.contents": {
            "verbose_name": "Meta Description",
            "type": STRING_TYPE,
            "settings": {
                ES_NOT_ANALYZED,
                ES_LIST,
                DIFF_QUALITATIVE
            }
        },
        # h2 tag
        "metadata.h2.nb": {
            "verbose_name": "No. of H2",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "metadata.h2.contents": {
            "verbose_name": "H2",
            "type": STRING_TYPE,
            "settings": {
                ES_NOT_ANALYZED,
                ES_LIST,
                DIFF_QUALITATIVE
            }
        },

        # h3 tag
        "metadata.h3.nb": {
            "verbose_name": "No. of H3",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "metadata.h3.contents": {
            "verbose_name": "H3",
            "type": STRING_TYPE,
            "settings": {
                ES_NOT_ANALYZED,
                ES_LIST,
                DIFF_QUALITATIVE
            }
        },
        "metadata.h1.len": {
            "verbose_name": "H1 Length",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL,
                FIELD_RIGHTS.FILTERS,
                FIELD_RIGHTS.SELECT,
                DIFF_QUANTITATIVE
            },
            "enabled": check_enabled("length")
        },
        "metadata.title.len": {
            "verbose_name": "Title Length",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL,
                FIELD_RIGHTS.FILTERS,
                FIELD_RIGHTS.SELECT,
                DIFF_QUANTITATIVE
            },
            "enabled": check_enabled("length")
        },
        "metadata.description.len": {
            "verbose_name": "Meta Description Length",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL,
                FIELD_RIGHTS.FILTERS,
                FIELD_RIGHTS.SELECT,
                DIFF_QUANTITATIVE
            },
            "enabled": check_enabled("length")
        },
    }

    def process_document(self, document, stream):
        content_type_id = stream[self.field_idx('content_type')]
        content_type = CONTENT_TYPE_INDEX[content_type_id]
        content = stream[self.field_idx('txt')]
        document['metadata'][content_type]['contents'].append(content)

    def post_process_document(self, document):
        for content_type in ('h1', 'title', 'description'):
            if not document['metadata'][content_type]['contents']:
                continue
            content = document['metadata'][content_type]['contents'][0]
            content_len = len(content.decode('utf-8', 'replace'))
            document['metadata'][content_type]['len'] = content_len


def _get_duplicate_document_mapping(metadata_list,
                                    duplicate_type,
                                    is_context_aware,
                                    order_seed):
    """Generate mapping for duplicate documents
    :param duplicate_type: the kind of duplicate.
                           this string will be used to generate the field types
    :type duplicate_type: str
    :param is_context_aware: generate for context-aware fields?
    :type is_context_aware: bool
    :param order_seed: the base number to use to generate the "order" settings
    :type order_seed: int
    :returns: dict
    """
    result = {}
    for i, metadata_type in enumerate(metadata_list):
        prefix = "metadata.{}.{}".format(metadata_type, duplicate_type)
        if is_context_aware:
            verbose_template = 'No. of Duplicate {} (Among Compliant URLs in Same Zone)'
        else:
            verbose_template = 'No. of Duplicate {} (Among All URLs)'

        result["{}.nb".format(prefix)] = {
            "verbose_name": verbose_template.format(
                metadata_type.title()
            ),
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL,
                FIELD_RIGHTS.FILTERS,
                FIELD_RIGHTS.SELECT,
                DIFF_QUANTITATIVE
            }
        }

        if is_context_aware:
            verbose_template = '1st Duplicate {} Found (Among Other Compliant URLs in Same Zone)'
        else:
            verbose_template = '1st Duplicate {} Found (Among All URLs)'
        result["{}.is_first".format(prefix)] = {
            "verbose_name": verbose_template.format(
                 metadata_type.title()
            ),
            "type": BOOLEAN_TYPE,
            "settings": {
                FIELD_RIGHTS.SELECT,
                FIELD_RIGHTS.FILTERS,
                DIFF_QUALITATIVE
            }
        }
        # template for samples field
        if is_context_aware:
            verbose_template = 'Sample of URLs with the Same {} (Among Other Compliant URLs in Same Zone)'
        else:
            verbose_template = 'Sample of URLs with the Same {} (Among All URLs)'
        result["{}.urls".format(prefix)] = {
            "verbose_name": verbose_template.format(metadata_type.title()),
            "type": INT_TYPE,
            "settings": {
                ES_NO_INDEX,
                ES_LIST,
                RENDERING.URL_STATUS,
                URL_ID,
                FIELD_RIGHTS.SELECT
            }
        }

        result["{}.urls_exists".format(prefix)] = {
            "type": "boolean",
            "default_value": None
        }

    return result


# the headers to use for duplicate stream defs
CONTENTSDUPLICATE_HEADERS = (
    ('id', int),
    ('content_type', int),
    ('duplicates_nb', int),
    ('is_first_url', _raw_to_bool),
    ('duplicate_urls', lambda k: [int(i) for i in k.split(';')] if k else [])
)


def _process_document_for_duplicates(duplicate_type, document, stream):
    """Process a document with duplicate data.
    This is a helper function which is intended to be used to define
    process_document() functions in duplicate stream def.
    :param duplicate_type: the type of duplicate that will be processed.
    :type duplicate_type: str
    :param document: the input document to update
    :type document: dict
    :param stream: a stream element
    :type stream: tuple
    """
    _, metadata_idx, nb_duplicates, is_first, duplicate_urls = stream
    metadata_type = CONTENT_TYPE_INDEX[metadata_idx]

    dup = get_subdict_from_path(
        "metadata.{}.{}".format(metadata_type, duplicate_type),
        document
    )
    # number of duplications of this piece of metadata
    dup['nb'] = nb_duplicates
    # urls that have duplicates
    if nb_duplicates > 0:
        dup['urls'] = duplicate_urls
        dup['urls_exists'] = True

    # is this the first one out of all duplicates
    dup['is_first'] = is_first


class ContentsDuplicateStreamDef(StreamDefBase):
    FILE = 'urlcontentsduplicate'
    HEADERS = CONTENTSDUPLICATE_HEADERS
    URL_DOCUMENT_DEFAULT_GROUP = "semantic_metadata"

    URL_DOCUMENT_MAPPING = _get_duplicate_document_mapping(
        ["title", "description", "h1"],
        "duplicates",
        is_context_aware=False,
        order_seed=100
    )

    def process_document(self, document, stream):
        _process_document_for_duplicates(
            "duplicates", document, stream
        )


class ContentsContextAwareDuplicateStreamDef(StreamDefBase):
    FILE = 'urlcontentsduplicate_contextaware'
    HEADERS = CONTENTSDUPLICATE_HEADERS
    URL_DOCUMENT_DEFAULT_GROUP = "semantic_metadata"

    URL_DOCUMENT_MAPPING = _get_duplicate_document_mapping(
        ["title", "description", "h1"],
        "duplicates.context_aware",
        is_context_aware=True,
        order_seed=200
    )

    def process_document(self, document, stream):
        _process_document_for_duplicates(
            "duplicates.context_aware", document, stream
        )


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
            "verbose_name": "No. of Titles",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                AGG_CATEGORICAL,
                DIFF_QUANTITATIVE
            }
        },
        # h1 tag
        "metadata.h1.nb": {
            "verbose_name": "No. of H1",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        # description tag
        "metadata.description.nb": {
            "verbose_name": "No. of Meta Descriptions",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        # h2 tag
        "metadata.h2.nb": {
            "verbose_name": "No. of H2",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL
            }
        },
        # h3 tag
        "metadata.h3.nb": {
            "verbose_name": "No. of H3",
            "type": INT_TYPE,
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
