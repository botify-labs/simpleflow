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
            [1, "organic", "google", "(not set)", 10, 9, 80.0, 4.0, 32.4, 45.0, 90.0],
            [1, "organic", "bing", "(not set)", 15, 15, 60.4, 3.2, 5.0, 88.12, 12.4],
            [3, "organic", "google", "(not set)", 7, 6, 50.41, 2.1, 60.41, 12.11, 80.41],
        ]

    #patch sources to be able to add sources without
    #having to change the test
    @mock.patch("cdf.features.ganalytics.streams.ORGANIC_SOURCES",
                ["google", "bing", "yahoo"])
    @mock.patch("cdf.features.ganalytics.streams.SOCIAL_SOURCES",
                ["facebook", "twitter", "pinterest"])
    def test_url_infos(self):
        gen = UrlDocumentGenerator([
            IdStreamDef.get_stream_from_iterator(iter(self.ids)),
            InfosStreamDef.get_stream_from_iterator(iter(self.infos)),
            VisitsStreamDef.get_stream_from_iterator(iter(self.visits)),
        ])
        documents = [k[1] for k in gen]

        self.assertEquals(
            documents[0]["visits"],
            {
                "organic": {
                    "google": {
                        "nb": 10
                    },
                    "bing": {
                        "nb": 15
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
        )

        self.assertEquals(
            documents[1]["visits"],
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
        )

        self.assertEquals(
            documents[2]["visits"],
            {
                "organic": {
                    "google": {
                        "nb": 7
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
        )
