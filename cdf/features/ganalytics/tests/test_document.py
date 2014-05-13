import unittest
import mock

from cdf.analysis.urls.generators.documents import UrlDocumentGenerator
from cdf.features.main.streams import IdStreamDef, InfosStreamDef
from cdf.features.ganalytics.streams import VisitsStreamDef


class TestBasicInfoGeneration(unittest.TestCase):
    def setUp(self):
        self.ids = [
            [1, 'http', 'www.site.com', '/path/name1.html', ''],
            [2, 'http', 'www.site.com', '/path/name2.html', ''],
            [3, 'http', 'www.site.com', '/path/name3.html', ''],
        ]
        self.infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [2, 2, 'text/html', 0, 1, 200, 1200, 303, 456],
            [3, 2, 'text/html', 0, 1, 200, 1200, 303, 456],
        ]

        self.visits = [
            [1, "organic", "google", "None", 10, 9, 9, 12, 32, 4, 9],
            [1, "organic", "bing", "None", 15, 15, 10, 20, 35, 8, 4],
            [3, "organic", "google", "None", 7, 6, 3, 6, 60, 5, 4],
            [3, "organic", "foo", "None", 2, 2, 1, 6, 10, 1, 0],
        ]

    #patch sources to be able to add sources without
    #having to change the test
    @mock.patch("cdf.features.ganalytics.streams.ORGANIC_SOURCES",
                ["google", "bing"])
    @mock.patch("cdf.features.ganalytics.streams.SOCIAL_SOURCES",
                ["facebook"])
    def test_url_infos(self):
        gen = UrlDocumentGenerator([
            IdStreamDef.get_stream_from_iterator(iter(self.ids)),
            InfosStreamDef.get_stream_from_iterator(iter(self.infos)),
            VisitsStreamDef.get_stream_from_iterator(iter(self.visits)),
        ])
        null_entry = {
            "nb": 0,
            "bounce_rate": 0,
            "pages_per_session": 0,
            "average_session_duration": 0,
            "percentage_new_sessions": 0,
            "goal_conversion_rate_all": 0
        }
        documents = [k[1] for k in gen]
        self.assertEquals(
            documents[0]["visits"],
            {
                "organic": {
                    "all": {
                        "nb": 25,
                        "bounce_rate": 79.17,
                        "pages_per_session": 1.33,
                        "average_session_duration": 2.79,
                        "percentage_new_sessions": 50,
                        "goal_conversion_rate_all": 54.17
                    },
                    "considered": {
                        "nb": 25,
                        "bounce_rate": 79.17,
                        "pages_per_session": 1.33,
                        "average_session_duration": 2.79,
                        "percentage_new_sessions": 50,
                        "goal_conversion_rate_all": 54.17
                    },
                    "google": {
                        "nb": 10,
                        "bounce_rate": 100,
                        "pages_per_session": 1.33,
                        "average_session_duration": 3.56,
                        "percentage_new_sessions": 44.44,
                        "goal_conversion_rate_all": 100
                    },
                    "bing": {
                        "nb": 15,
                        "bounce_rate": 66.67,
                        "pages_per_session": 1.33,
                        "average_session_duration": 2.33,
                        "percentage_new_sessions": 53.33,
                        "goal_conversion_rate_all": 26.67
                    },
                },
                "social": {
                    "all": null_entry,
                    "considered": null_entry,
                    "facebook": null_entry,
                }
            }
        )

        self.assertEquals(
            documents[1]["visits"],
            {
                "organic": {
                    "all": null_entry,
                    "considered": null_entry,
                    "google": null_entry,
                    "bing": null_entry,
                },
                "social": {
                    "all": null_entry,
                    "considered": null_entry,
                    "facebook": null_entry,
                }
            }
        )

        self.assertEquals(
            documents[2]["visits"],
            {
                "organic": {
                    "all": {
                        "nb": 9,
                        "bounce_rate": 50,
                        "pages_per_session": 1.5,
                        "average_session_duration": 8.75,
                        "percentage_new_sessions": 75,
                        "goal_conversion_rate_all": 50
                    },
                    "considered": {
                        "nb": 7,
                        "bounce_rate": 50,
                        "pages_per_session": 1,
                        "average_session_duration": 10,
                        "percentage_new_sessions": 83.33,
                        "goal_conversion_rate_all": 66.67
                    },
                    "google": {
                        "nb": 7,
                        "bounce_rate": 50,
                        "pages_per_session": 1,
                        "average_session_duration": 10,
                        "percentage_new_sessions": 83.33,
                        "goal_conversion_rate_all": 66.67
                    },
                    "bing": null_entry,
                },
                "social": {
                    "all": null_entry,
                    "considered": null_entry,
                    "facebook": null_entry,
                }
            }
        )
