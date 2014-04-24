from cdf.metadata.url.url_metadata import (
    INT_TYPE, ES_DOC_VALUE, AGG_NUMERICAL
)
from cdf.core.features import StreamDefBase


class RawVisitsStreamDef(StreamDefBase):
    FILE = 'analytics_raw_data'
    HEADERS = (
        ('url', str),
        ('medium', str),
        ('source', str),
        ('nb_visits', int),
    )


class VisitsStreamDef(StreamDefBase):
    FILE = 'analytics_data'
    HEADERS = (
        ('id', int),
        ('medium', str),
        ('source', str),
        ('nb_visits', int),
    )
    URLS_DOCUMENT_MAPPING = {
        "visits.organic.google.nb": {
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL
            }
        },
        "visits.organic.bing.nb": {
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL
            }
        }
    }

    def pre_process_document(self, document):
        document["visits"] = {
            "organic": {
                "google": {
                    "nb": 0
                },
                "bing": {
                    "nb": 0
                }
            }
        }

    def process_document(self, document, stream):
        _, medium, source, nb_visits = stream
        if medium != 'organic' or source not in ('google', 'bing'):
            return
        document['visits'][medium][source]['nb'] = nb_visits
