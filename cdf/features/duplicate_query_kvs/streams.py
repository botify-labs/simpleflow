from cdf.core.metadata.constants import FIELD_RIGHTS, RENDERING
from cdf.core.streams.base import StreamDefBase
from cdf.metadata.url.url_metadata import INT_TYPE, ES_NO_INDEX, ES_LIST, URL_ID, \
    AGG_NUMERICAL, ES_DOC_VALUE, DIFF_QUANTITATIVE, BOOLEAN_TYPE
from .settings import GROUPS
from cdf.utils.convert import str_to_int_list


class DuplicateQueryKVsStreamDef(StreamDefBase):
    FILE = 'duplicate_query_kvs'
    HEADERS = (
        ('id', int),
        ('ids', str_to_int_list),  # id2 id3...
    )
    URL_DOCUMENT_DEFAULT_GROUP = GROUPS.duplicate_query_kvs.name

    URL_DOCUMENT_MAPPING = {
        "duplicate_query_kvs.urls": {
            "type": INT_TYPE,
            "verbose_name": "Sample of other Urls with the same key/values in a different order.",
            "settings": {
                ES_DOC_VALUE,
                ES_NO_INDEX,
                ES_LIST,
                FIELD_RIGHTS.SELECT,
                FIELD_RIGHTS.FILTERS,
                RENDERING.URL,
                URL_ID
            },
        },
        "duplicate_query_kvs.nb": {
            "type": INT_TYPE,
            "verbose_name": "Number of Urls with the same key/values in a different order.",
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE,
                FIELD_RIGHTS.SELECT,
                FIELD_RIGHTS.FILTERS,
            },
        },
        "duplicate_query_kvs.urls_exists": {
            "type": BOOLEAN_TYPE,
            "default_value": None
        },
    }

    def process_document(self, document, stream):
        id1, ids = stream
        if ids:
            document['duplicate_query_kvs']['urls'] = sorted(ids)[:10]
            document['duplicate_query_kvs']['nb'] = len(ids) + 1
            document['duplicate_query_kvs']['urls_exists'] = True
