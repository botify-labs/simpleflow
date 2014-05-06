import unittest
import mock
from cdf.metadata.url.url_metadata import (
    INT_TYPE, ES_DOC_VALUE, AGG_NUMERICAL
)
from cdf.features.ganalytics.settings import ORGANIC_SOURCES, SOCIAL_SOURCES
from cdf.features.ganalytics.streams import (VisitsStreamDef,
                                             _get_url_document_mapping)


class TestVisitsStreamDef(unittest.TestCase):
    #patch organic sources to be able to add organic sources without
    #having to change the sources
    @mock.patch("cdf.features.ganalytics.streams.ORGANIC_SOURCES",
                ["google", "bing", "yahoo"])
    def test_preprocess(self):
        document = {}
        VisitsStreamDef().pre_process_document(document)
        expected_document = {
            "visits":
            {
                "organic": {
                    "google": {
                        "nb": 0
                    },
                    "bing": {
                        "nb": 0
                    },
                    "yahoo": {
                        "nb": 0
                    }
                },
                "social": {
                    "facebook": {
                        "nb": 0
                    },
                    "twitter": {
                        "nb": 0
                    },
                    "pinterest": {
                        "nb": 0
                    }
                }
            }
        }
        self.assertEqual(expected_document, document)

    def test_get_url_document_mapping(self):
        expected_mapping = {
            "visits.organic.google.nb": {
                "type": INT_TYPE,
                "settings": {
                    ES_DOC_VALUE,
                    AGG_NUMERICAL
                }
            },
            "visits.organic.yahoo.nb": {
                "type": INT_TYPE,
                "settings": {
                    ES_DOC_VALUE,
                    AGG_NUMERICAL
                }
            },
            "visits.social.facebook.nb": {
                "type": INT_TYPE,
                "settings": {
                    ES_DOC_VALUE,
                    AGG_NUMERICAL
                }
            },
            "visits.social.twitter.nb": {
                "type": INT_TYPE,
                "settings": {
                    ES_DOC_VALUE,
                    AGG_NUMERICAL
                }
            },
        }
        organic_sources = ["google", "yahoo"]
        social_sources = ["facebook", "twitter"]
        actual_mapping = _get_url_document_mapping(organic_sources,
                                                   social_sources)
        self.assertEqual(expected_mapping, actual_mapping)
