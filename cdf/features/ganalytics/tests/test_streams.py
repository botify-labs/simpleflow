import unittest
import mock
from cdf.metadata.url.url_metadata import (
    INT_TYPE, FLOAT_TYPE, ES_DOC_VALUE, AGG_NUMERICAL
)
from cdf.features.ganalytics.streams import (VisitsStreamDef,
                                             _get_url_document_mapping)


class TestVisitsStreamDef(unittest.TestCase):
    #patch organic and social sources to be able to add sources without
    #having to change the test
    @mock.patch("cdf.features.ganalytics.streams.ORGANIC_SOURCES",
                ["google", "bing", "yahoo"])
    @mock.patch("cdf.features.ganalytics.streams.SOCIAL_SOURCES",
                ["facebook", "twitter", "pinterest"])
    def test_preprocess(self):
        document = {}
        VisitsStreamDef().pre_process_document(document)
        expected_document = {
            "visits":
            {
                "organic": {
                    "google": {
                        "nb": 0,
                        "sessions": 0,
                        "bounces": 0,
                        "page_views": 0
                    },
                    "bing": {
                        "nb": 0,
                        "sessions": 0,
                        "bounces": 0,
                        "page_views": 0
                    },
                    "yahoo": {
                        "nb": 0,
                        "sessions": 0,
                        "bounces": 0,
                        "page_views": 0
                    }
                },
                "social": {
                    "facebook": {
                        "nb": 0,
                        "sessions": 0,
                        "bounces": 0,
                        "page_views": 0
                    },
                    "twitter": {
                        "nb": 0,
                        "sessions": 0,
                        "bounces": 0,
                        "page_views": 0
                    },
                    "pinterest": {
                        "nb": 0,
                        "sessions": 0,
                        "bounces": 0,
                        "page_views": 0
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
            "visits.organic.google.bounce_rate": {
                "type": FLOAT_TYPE,
                "settings": {
                    ES_DOC_VALUE,
                    AGG_NUMERICAL
                }
            },
            "visits.organic.google.pages_per_session": {
                "type": FLOAT_TYPE,
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
            "visits.organic.yahoo.bounce_rate": {
                "type": FLOAT_TYPE,
                "settings": {
                    ES_DOC_VALUE,
                    AGG_NUMERICAL
                }
            },
            "visits.organic.yahoo.pages_per_session": {
                "type": FLOAT_TYPE,
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
            "visits.social.facebook.bounce_rate": {
                "type": FLOAT_TYPE,
                "settings": {
                    ES_DOC_VALUE,
                    AGG_NUMERICAL
                }
            },
        "visits.social.facebook.pages_per_session": {
            "type": FLOAT_TYPE,
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
        "visits.social.twitter.bounce_rate": {
            "type": FLOAT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL
            }
        },
        "visits.social.twitter.pages_per_session": {
            "type": FLOAT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL
            }
        }
        }
        organic_sources = ["google", "yahoo"]
        social_sources = ["facebook", "twitter"]
        actual_mapping = _get_url_document_mapping(organic_sources,
                                                   social_sources)
        self.assertEqual(expected_mapping, actual_mapping)

    def test_compute_metrics_nominal_case(self):
        input_d = {
            "sessions": 3,
            "bounces": 2,
            "page_views": 6
        }
        VisitsStreamDef().compute_metrics(input_d)
        expected_result = {
            "sessions": 3,
            "bounces": 2,
            "bounce_rate": 66.67,
            "page_views": 6,
            "pages_per_session": 2
        }
        self.assertEqual(expected_result, input_d)

    def test_delete_intermediary_metrics_nominal_case(self):
        input_d = {
            "foo": 2,
            "bounces": 3,
            "sessions": 4,
            "page_views": 5,
        }
        VisitsStreamDef().delete_intermediary_metrics(input_d)
        expected_result = {
            "foo": 2
        }

        self.assertEqual(expected_result, input_d)

    def test_delete_intermediary_metrics_missing_keys(self):
        #"sessions" key is missing
        input_d = {
            "foo": 2,
            "bounces": 3,
        }
        VisitsStreamDef().delete_intermediary_metrics(input_d)
        expected_result = {
            "foo": 2
        }

        self.assertEqual(expected_result, input_d)

    def test_compute_bounce_rate(self):
        stream = VisitsStreamDef()
        self.assertEqual(66.67, stream.compute_bounce_rate(2, 3))
        self.assertEqual(0, stream.compute_bounce_rate(2, 0))

    def test_compute_pages_per_session(self):
        stream = VisitsStreamDef()
        self.assertEqual(1.33, stream.compute_pages_per_session(4, 3))
        self.assertEqual(0, stream.compute_pages_per_session(4, 0))
