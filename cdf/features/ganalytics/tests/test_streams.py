import unittest
from cdf.features.ganalytics.streams import VisitsStreamDef


class TestVisitsStreamDef(unittest.TestCase):
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


