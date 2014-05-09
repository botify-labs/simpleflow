import unittest
import mock
from cdf.metadata.url.url_metadata import (
    INT_TYPE, FLOAT_TYPE, ES_DOC_VALUE, AGG_NUMERICAL
)
from cdf.features.ganalytics.settings import ORGANIC_SOURCES, SOCIAL_SOURCES
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
                        "page_views": 0,
                        "session_duration": 0,
                        "new_users": 0,
                        "goal_completions_all": 0
                    },
                    "bing": {
                        "nb": 0,
                        "sessions": 0,
                        "bounces": 0,
                        "page_views": 0,
                        "session_duration": 0,
                        "new_users": 0,
                        "goal_completions_all": 0
                    },
                    "yahoo": {
                        "nb": 0,
                        "sessions": 0,
                        "bounces": 0,
                        "page_views": 0,
                        "session_duration": 0,
                        "new_users": 0,
                        "goal_completions_all": 0
                    }
                },
                "social": {
                    "facebook": {
                        "nb": 0,
                        "sessions": 0,
                        "bounces": 0,
                        "page_views": 0,
                        "session_duration": 0,
                        "new_users": 0,
                        "goal_completions_all": 0
                    },
                    "twitter": {
                        "nb": 0,
                        "sessions": 0,
                        "bounces": 0,
                        "page_views": 0,
                        "session_duration": 0,
                        "new_users": 0,
                        "goal_completions_all": 0
                    },
                    "pinterest": {
                        "nb": 0,
                        "sessions": 0,
                        "bounces": 0,
                        "page_views": 0,
                        "session_duration": 0,
                        "new_users": 0,
                        "goal_completions_all": 0
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
            },
        }
        organic_sources = ["google", "yahoo"]
        social_sources = ["facebook", "twitter"]
        metrics = [
            "bounce_rate",
            "pages_per_session",
        ]
        actual_mapping = _get_url_document_mapping(organic_sources,
                                                   social_sources,
                                                   metrics)
        self.assertEqual(expected_mapping, actual_mapping)

    def test_url_document_mapping(self):
        expected_mapping = _get_url_document_mapping(ORGANIC_SOURCES,
                                                     SOCIAL_SOURCES,
                                                     VisitsStreamDef._METRICS)

        self.assertEqual(expected_mapping,
                         VisitsStreamDef.URL_DOCUMENT_MAPPING)
                         

    def test_compute_metrics_nominal_case(self):
        input_d = {
            "sessions": 3,
            "bounces": 2,
            "page_views": 6,
            "session_duration": 8,
            "new_users": 2,
            "goal_completions_all": 1
        }
        VisitsStreamDef().compute_metrics(input_d)
        expected_result = {
            "sessions": 3,
            "bounces": 2,
            "bounce_rate": 66.67,
            "page_views": 6,
            "pages_per_session": 2,
            "session_duration": 8,
            "average_session_duration": 2.67,
            "new_users": 2,
            "percentage_new_sessions": 66.67,
            "goal_completions_all": 1,
            "goal_conversion_rate_all": 33.33
        }
        self.assertEqual(expected_result, input_d)

    def test_delete_intermediary_metrics_nominal_case(self):
        input_d = {
            "foo": 2,
            "bounces": 3,
            "sessions": 4,
            "page_views": 5,
            "session_duration": 6,
            "new_users": 7,
            "goal_completions_all": 8
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

    def test_compute_average_session_duration(self):
        stream = VisitsStreamDef()
        self.assertEqual(2.33, stream.compute_average_session_duration(7, 3))
        self.assertEqual(0, stream.compute_average_session_duration(4, 0))

    def test_compute_percentage_new_session(self):
        stream = VisitsStreamDef()
        self.assertEqual(42.86, stream.compute_percentage_new_sessions(3, 7))
        self.assertEqual(0, stream.compute_percentage_new_sessions(7, 0))

    def test_compute_goal_conversion_rate(self):
        stream = VisitsStreamDef()
        self.assertEqual(37.5, stream.compute_goal_conversion_rate(3, 8))
        self.assertEqual(0, stream.compute_goal_conversion_rate(7, 0))
