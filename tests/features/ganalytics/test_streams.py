import unittest
import mock
from cdf.metadata.url.url_metadata import (
    INT_TYPE, FLOAT_TYPE, ES_DOC_VALUE, AGG_NUMERICAL,
    DIFF_QUANTITATIVE
)
from cdf.features.ganalytics.settings import ORGANIC_SOURCES, SOCIAL_SOURCES
from cdf.features.ganalytics.streams import (VisitsStreamDef,
                                             _iterate_sources,
                                             _get_url_document_mapping,
                                             _update_document_mapping)


class TestIterateSources(unittest.TestCase):
    #patch organic and social sources to be able to add sources without
    #having to change the test
    @mock.patch("cdf.features.ganalytics.streams.ORGANIC_SOURCES",
                ["google", "bing", "yahoo"])
    @mock.patch("cdf.features.ganalytics.streams.SOCIAL_SOURCES",
                ["facebook", "twitter", "pinterest"])
    def test_nominal_case(self):
        expected_result = [
            ("organic", "all"),
            ("organic", "google"),
            ("organic", "bing"),
            ("organic", "yahoo"),
            ("social", "all"),
            ("social", "facebook"),
            ("social", "twitter"),
            ("social", "pinterest")
        ]
        self.assertEqual(expected_result, list(_iterate_sources()))


class TestUpdateDocumentMapping(unittest.TestCase):
    def test_update_document_mapping_sources_parameter(self):
        mapping = {}
        expected_mapping = {
            "visits.organic.all.nb": {
                "type": INT_TYPE,
                "settings": {
                    ES_DOC_VALUE,
                    AGG_NUMERICAL,
                    DIFF_QUANTITATIVE
                }
            },
            "visits.organic.google.nb": {
                "type": INT_TYPE,
                "settings": {
                    ES_DOC_VALUE,
                    AGG_NUMERICAL,
                    DIFF_QUANTITATIVE
                }
            },
            "visits.organic.yahoo.nb": {
                "type": INT_TYPE,
                "settings": {
                    ES_DOC_VALUE,
                    AGG_NUMERICAL,
                    DIFF_QUANTITATIVE
                }
            },
        }
        organic_sources = ["google", "yahoo"]
        metrics = [
            ("nb", None, "nb", "Number of visits from {source}", INT_TYPE, None),
        ]
        _update_document_mapping(mapping, "organic",
                                 organic_sources, metrics)
        self.assertItemsEqual(mapping.keys(), ["visits.organic.all.nb", "visits.organic.google.nb", "visits.organic.yahoo.nb"])
        self.assertEquals(
            sorted([(k, v["settings"]) for k, v in expected_mapping.iteritems()]),
            sorted([(k, v["settings"]) for k, v in mapping.iteritems()])
        )

    def test_update_document_mapping_empty_sources_parameter(self):
        mapping = {}
        expected_mapping = {
            "visits.organic.all.nb": {
                "type": INT_TYPE,
                "settings": {
                    ES_DOC_VALUE,
                    AGG_NUMERICAL,
                    DIFF_QUANTITATIVE
                },
                "verbose_name": "Number of visits on organic",
                "order": 0
            }
        }
        organic_sources = []
        metrics = [
            ("nb", None, "nb", "Number of visits from {source}", INT_TYPE, None),
        ]
        _update_document_mapping(mapping, "organic",
                                 organic_sources, metrics)
        self.assertEquals(mapping.keys(), ["visits.organic.all.nb"])
        self.assertEqual(mapping["visits.organic.all.nb"]["settings"], expected_mapping["visits.organic.all.nb"]["settings"])

    def test_update_document_mapping_metrics_parameters(self):
        mapping = {}
        expected_mapping = {
            "visits.organic.all.nb": {
                "type": INT_TYPE,
                "order": 0,
                "verbose_name": "Number of visits on organic",
                "group": "visits.organic.all",
                "settings": {
                    ES_DOC_VALUE,
                    AGG_NUMERICAL,
                    DIFF_QUANTITATIVE
                }
            },
            "visits.organic.all.bounce_rate": {
                "type": FLOAT_TYPE,
                "order": 1,
                "verbose_name": "Bounce Rate on organic",
                "group": "visits.organic.all",
                "settings": {
                    ES_DOC_VALUE,
                    AGG_NUMERICAL,
                    DIFF_QUANTITATIVE
                }
            },
            "visits.organic.all.pages_per_session": {
                "type": FLOAT_TYPE,
                "order": 2,
                "verbose_name": "Pages per sesssion on organic",
                "group": "visits.organic.all",
                "settings": {
                    ES_DOC_VALUE,
                    AGG_NUMERICAL,
                    DIFF_QUANTITATIVE
                }
            }
        }
        organic_sources = []
        metrics = [
            ("nb", None, "nb", "Number of visits from {source}", INT_TYPE, None),
            ("bounce_rate", lambda i:i, "bounces", "Bounce Rate", FLOAT_TYPE, None),
            ("pages_per_session", lambda i:i, "page_views", "Pages per session", FLOAT_TYPE, None),
        ]
        _update_document_mapping(mapping, "organic",
                                 organic_sources, metrics)
        self.assertEquals(
            sorted([(k, v["settings"]) for k, v in expected_mapping.iteritems()]),
            sorted([(k, v["settings"]) for k, v in mapping.iteritems()])
        )


class TestGetUrlDocumentMapping(unittest.TestCase):
    @mock.patch("cdf.features.ganalytics.streams._update_document_mapping")
    def test_get_url_document_mapping_organic_parameter(self, mock):
        organic_sources = ["google", "yahoo"]
        social_sources = []
        metrics = []
        actual_mapping = _get_url_document_mapping(organic_sources,
                                                   social_sources,
                                                   metrics)
        self.assertIsInstance(actual_mapping, dict)
        mock.assert_called_once_with({}, "organic", ["google", "yahoo"], [])

    @mock.patch("cdf.features.ganalytics.streams._update_document_mapping")
    def test_get_url_document_mapping_social_parameter(self, mock):
        organic_sources = []
        social_sources = ["facebook", "twitter"]
        metrics = []
        actual_mapping = _get_url_document_mapping(organic_sources,
                                                   social_sources,
                                                   metrics)
        self.assertIsInstance(actual_mapping, dict)
        mock.assert_called_once_with({}, "social", ["facebook", "twitter"], [])

    @mock.patch("cdf.features.ganalytics.streams._update_document_mapping")
    def test_get_url_document_mapping_nominal_case(self, mock):
        organic_sources = ["google"]
        social_sources = ["twitter"]
        metrics = ["bounces"]
        actual_mapping = _get_url_document_mapping(organic_sources,
                                                   social_sources,
                                                   metrics)
        self.assertIsInstance(actual_mapping, dict)
        expected_calls = [
            mock.call({}, "organic", ["google"], []),
            mock.call({}, "social", ["twitter"], [])
        ]
        mock.assert_call_with(expected_calls)


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
        entry = {
            "nb": 0,
            "bounces": 0,
            "page_views": 0,
            "session_duration": 0,
            "new_users": 0,
            "goal_completions_all": 0
        }
        expected_document = {
            "visits":
            {
                "organic": {
                    "all": dict(entry),
                    "google": dict(entry),
                    "bing": dict(entry),
                    "yahoo": dict(entry)
                },
                "social": {
                    "all": dict(entry),
                    "facebook": dict(entry),
                    "twitter": dict(entry),
                    "pinterest": dict(entry)
                }
            }
        }
        self.assertEqual(expected_document, document)

    def test_url_document_mapping(self):
        expected_mapping = _get_url_document_mapping(ORGANIC_SOURCES,
                                                     SOCIAL_SOURCES,
                                                     VisitsStreamDef._METRICS)

        self.assertEqual(expected_mapping,
                         VisitsStreamDef.URL_DOCUMENT_MAPPING)

    def test_update_entry(self):
        entry = {
            "nb": 1,
            "bounces": 2,
            "page_views": 3,
            "session_duration": 4,
            "new_users": 5,
            "goal_completions_all": 6
        }
        stream_line = [0, "organic", "google", "None", 7, 6, 5, 4, 3, 2, 1]

        expected_result = {
            "nb": 8,
            "bounces": 8,
            "page_views": 8,
            "session_duration": 8,
            "new_users": 8,
            "goal_completions_all": 8
        }
        VisitsStreamDef().update_entry(entry, stream_line)
        self.assertEquals(expected_result, entry)

    def test_consider_source(self):
        stream = [0, "organic", "google", "None"]
        self.assertTrue(VisitsStreamDef().consider_source(stream))

        #considerd search engine
        stream = [0, "organic", "foo", "None"]
        self.assertFalse(VisitsStreamDef().consider_source(stream))

        #non organic google traffic
        stream = [0, "(cpc)", "google", "None"]
        self.assertFalse(VisitsStreamDef().consider_source(stream))

        stream = [0, "social", "twitter.com", "twitter"]
        self.assertTrue(VisitsStreamDef().consider_source(stream))

        #non "social" traffic from a social network
        stream = [0, "referral", "t.co", "twitter"]
        self.assertTrue(VisitsStreamDef().consider_source(stream))

        #considerd social network
        stream = [0, "social", "twitter.com", "foo"]
        self.assertFalse(VisitsStreamDef().consider_source(stream))

    def test_get_visit_medium_source(self):
        stream = [0, "organic", "google", "None"]
        self.assertEqual(("organic", "google"),
                         VisitsStreamDef().get_visit_medium_source(stream))
        stream = [0, "referral", "t.co", "twitter"]
        self.assertEqual(("social", "twitter"),
                         VisitsStreamDef().get_visit_medium_source(stream))

        #ignored search engine
        stream = [0, "organic", "foo", "None"]
        self.assertEqual(("organic", "foo"),
                         VisitsStreamDef().get_visit_medium_source(stream))

        #referral traffic with no social network
        stream = [0, "referral", "foo", "None"]
        self.assertEqual((None, None),
                         VisitsStreamDef().get_visit_medium_source(stream))

    def test_process_document_organic_source(self):
        document = {
            "visits": {
                "organic": {
                    "all": {
                        "nb": 1,
                        "bounces": 2,
                        "page_views": 3,
                        "session_duration": 4,
                        "new_users": 5,
                        "goal_completions_all": 6
                    },
                    "google": {
                        "nb": 6,
                        "bounces": 5,
                        "page_views": 4,
                        "session_duration": 3,
                        "new_users": 2,
                        "goal_completions_all": 1
                    }
                }
            }
        }
        stream = [0, "organic", "google", "None", 1, 2, 3, 4, 5, 6]
        VisitsStreamDef().process_document(document, stream)
        expected_document = {
            "visits": {
                "organic": {
                    "all": {
                        "nb": 2,
                        "bounces": 4,
                        "page_views": 6,
                        "session_duration": 8,
                        "new_users": 10,
                        "goal_completions_all": 12
                    },
                    "google": {
                        "nb": 7,
                        "bounces": 7,
                        "page_views": 7,
                        "session_duration": 7,
                        "new_users": 7,
                        "goal_completions_all": 7
                    }
                }
            }
        }
        print document["visits"]["organic"]["all"]
        self.assertEqual(expected_document, document)

    def test_process_document_ignored_organic_source(self):
        document = {
            "visits": {
                "organic": {
                    "all": {
                        "nb": 1,
                        "bounces": 2,
                        "page_views": 3,
                        "session_duration": 4,
                        "new_users": 5,
                        "goal_completions_all": 6
                    },
                    "google": {
                        "nb": 6,
                        "bounces": 5,
                        "page_views": 4,
                        "session_duration": 3,
                        "new_users": 2,
                        "goal_completions_all": 1
                    }
                }
            }
        }
        stream = [0, "organic", "foo", "None", 1, 2, 3, 4, 5, 6, 7]
        VisitsStreamDef().process_document(document, stream)
        #only the "all" entry has been updated
        expected_document = {
            "visits": {
                "organic": {
                    "all": {
                        "nb": 2,
                        "bounces": 4,
                        "page_views": 6,
                        "session_duration": 8,
                        "new_users": 10,
                        "goal_completions_all": 12
                    },
                    "google": {
                        "nb": 6,
                        "bounces": 5,
                        "page_views": 4,
                        "session_duration": 3,
                        "new_users": 2,
                        "goal_completions_all": 1
                    }
                }
            }
        }
        self.assertEqual(expected_document, document)

    def test_compute_metrics_nominal_case(self):
        input_d = {
            "nb": 3,
            "bounces": 2,
            "page_views": 6,
            "session_duration": 8,
            "new_users": 2,
            "goal_completions_all": 1
        }
        VisitsStreamDef().compute_calculated_metrics(input_d)
        expected_result = {
            "nb": 3,
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
            "page_views": 4,
            "session_duration": 5,
            "new_users": 6,
            "goal_completions_all": 7
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


