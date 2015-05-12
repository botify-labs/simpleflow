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
            "verbose_name": "Number of Page Titles",
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
            "verbose_name": "Number of H1",
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
            "verbose_name": "Number of Page Description",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "metadata.description.contents": {
            "verbose_name": "Page description",
            "type": STRING_TYPE,
            "settings": {
                ES_NOT_ANALYZED,
                ES_LIST,
                DIFF_QUALITATIVE
            }
        },
        # h2 tag
        "metadata.h2.nb": {
            "verbose_name": "Number of H2",
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
            "verbose_name": "Number of H3",
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
            "default_value": None,
            "settings": {
                FIELD_RIGHTS.ADMIN,
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
            "default_value": None,
            "settings": {
                FIELD_RIGHTS.ADMIN,
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
            "verbose_name": "Description Length",
            "type": INT_TYPE,
            "default_value": None,
            "settings": {
                FIELD_RIGHTS.ADMIN,
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
            document['metadata'][content_type]['len'] = len(content)


def _get_duplicate_document_mapping(metadata_list,
                                    duplicate_type,
                                    verbose_prefix,
                                    order_seed):
    """Generate mapping for duplicate documents
    :param duplicate_type: the kind of duplicate.
                           this string will be used to generate the field types
    :type duplicate_type: str
    :type verbose_prefix: the prefix to apply to the verbose description
                          of the duplicate type.
                          This string will be used to generate the
                          verbose names.
    :type verbose_prefix: str
    :param order_seed: the base number to use to generate the "order" settings
    :type order_seed: int
    :param private: if True, all the generated fields will be private
    :type private: bool
    :returns: dict
    """
    result = {}
    verbose_duplicate_type = "duplicate"
    if len(verbose_prefix) > 0:
        verbose_duplicate_type = "{} {}".format(verbose_prefix, verbose_duplicate_type)

    for i, metadata_type in enumerate(metadata_list):
        prefix = "metadata.{}.{}".format(metadata_type, duplicate_type)
        result["{}.nb".format(prefix)] = {
            "verbose_name": "Number of {} {}".format(
                verbose_duplicate_type, metadata_type.capitalize()
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
        result["{}.is_first".format(prefix)] = {
            "verbose_name": "First {} {} found".format(
                verbose_duplicate_type, metadata_type.capitalize()
            ),
            "type": BOOLEAN_TYPE,
            "settings": {
                FIELD_RIGHTS.SELECT,
                FIELD_RIGHTS.FILTERS,
                DIFF_QUALITATIVE
            }
        }
        same_metadata_type = metadata_type.capitalize()
        if len(verbose_prefix) > 0:
            same_metadata_type = "{} {}".format(verbose_prefix, same_metadata_type)
        result["{}.urls".format(prefix)] = {
            "verbose_name": "Pages with the same {}".format(same_metadata_type),
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


#the headers to use for duplicate stream defs
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
        "",
        100
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
        "context-aware",
        200
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
            "verbose_name": "Number of Page Titles",
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
            "verbose_name": "Number of H1",
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
            "verbose_name": "Number of Page Description",
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
            "verbose_name": "Number of H2",
            "type": INT_TYPE,
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
