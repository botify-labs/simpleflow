# -*- coding: utf-8 -*-
import unittest

from cdf.exceptions import BotifyQueryException
from cdf.metadata.url.es_backend_utils import ElasticSearchBackend
from cdf.query.query_parsing import QueryParser
from cdf.tasks.documents import UrlDocumentGenerator
from cdf.features.main.streams import IdStreamDef
from cdf.features.extract.streams import ExtractResultsStreamDef
from cdf.query.datamodel import get_fields, get_groups

_stream1 = [
    # docid, name, label, agg, cast, rank, value
    (1, 'img-ALT', "extract_s_0", "list", "s", 1, ""),
    (1, 'img-ALT', "extract_s_0", "list", "s", 2, ""),
    (1, "html-truncate", "extract_s_1", "first", "", 0, r'<html class="ie"><![endif]-->\n<!--[if IE 9]><html class="ie9"><![endif]-->\n<!--'),
    (1, "titleFULL", "extract_s_2", "first", "", 0, r'<title>Le Monde.fr - Actualité à la Une</title>'),
    (1, "count", "extract_i_0", "count", "i", 0, 179),
    (1, 'img-ALT', "extract_s_0", "list", "s", 0, "Avatar lemonde.fr"),
    (2, 'img-ALT', "extract_s_0", "list", "s", 1, ""),
    (2, 'img-ALT', "extract_s_0", "list", "s", 2, ""),
    (2, "html-truncate", "extract_s_1", "first", "", 0, r'<html class="ie"><![endif]-->\n<!--[if IE 9]><html class="ie9"><![endif]-->\n<!--'),
    (2, "titleFULL", "extract_s_2", "first", "", 0, r'<title>Le Monde.fr - Actualité à la Une</title>'),
    (2, "count", "extract_i_0", "count", "i", 0, 5),
    (2, 'img-ALT', "extract_s_0", "list", "s", 0, ""),
]

_ids1 = [
    [1, 'http', 'www.site.com', '/path/name.html', ''],
    [2, 'http', 'www.site.com', '/path/other_name.html', ''],
]


class TestExtractResultsStreamDef(unittest.TestCase):
    def test_basic(self):
        gen = UrlDocumentGenerator(
            [IdStreamDef.load_iterator(iter(_ids1)), ExtractResultsStreamDef.load_iterator(iter(_stream1))])
        documents = list(gen)
        self.assertEqual(len(documents), 2)
        doc1 = documents[0][1]
        self.assertIn("extract", doc1)
        extract_ = doc1["extract"]
        self.assertIn("extract_s_0", extract_)
        self.assertEqual(extract_["extract_s_0"], ["Avatar lemonde.fr", "", ""])
        self.assertEqual(extract_["extract_s_2"], "<title>Le Monde.fr - Actualité à la Une</title>")
        self.assertEqual(extract_["extract_i_0"], 179)

    def test_rank(self):
        s = (1, "toto", "extract_s_0", "list", "s", 9, "thing")
        doc = {"extract": {"extract_s_0": None}}
        sd = ExtractResultsStreamDef()
        sd.process_document(doc, s)
        self.assertEqual(10, len(doc["extract"]["extract_s_0"]))
        self.assertEqual("thing", doc["extract"]["extract_s_0"][9])


class TestExtractFields(unittest.TestCase):
    def test(self):
        features_options = {
            "extract": [
                {
                    "name": "Product Prices",
                    "agg": "list",  # should return a multiple flag
                    "cast": "i",
                    "es_field": "extract_i_0"
                }
            ]
        }
        fields = get_fields(features_options)
        product_prices_field = filter(lambda f: f["value"].endswith("extract_i_0"), fields)[0]
        self.assertTrue(product_prices_field["multiple"])
        self.assertEquals(product_prices_field["name"], "Product Prices")
        self.assertEquals(product_prices_field["group"], "extract")

        # Test enabled fields
        # As we generate 5 fields for each type (i, f, s, b) we hide all
        # fields that are not reserved
        filters = [f["value"] for f in filter(lambda f: f["value"].startswith("extract."), fields)]
        self.assertEquals(filters, ["extract.extract_i_0"])

    def test_not_admin(self):
        features_options = {
            "extract": [
                {
                    "name": "Product Price",
                    "agg": "first",
                    "cast": "f",
                    "es_field": "extract_f_0"
                }
            ]
        }
        g = get_groups(features_options)
        self.assertEqual(1, len(g))

    def test_with_previous(self):
        features_options = {
            "extract": [
                {
                    "name": "Product Prices",
                    "agg": "list",  # should return a multiple flag
                    "cast": "i",
                    "es_field": "extract_i_0"
                }
            ],
            'comparison': {
                "options": {
                "extract": [
                    {
                        "name": "Product Prices",
                        "agg": "list",  # should return a multiple flag
                        "cast": "i",
                        "es_field": "extract_i_0"
                    }
                ],
            }
            }

        }
        fields = get_fields(features_options)
        product_prices_field = filter(lambda f: f["value"].endswith("extract_i_0"), fields)[0]
        self.assertTrue(product_prices_field["multiple"])
        self.assertEquals(product_prices_field["name"], "Product Prices")
        self.assertEquals(product_prices_field["group"], "extract")

        product_prices_field = filter(lambda f: f["value"].endswith("extract_i_0"), fields)[1]
        self.assertTrue(product_prices_field["multiple"])
        self.assertEquals(product_prices_field["name"], "Previous Product Prices")
        self.assertEquals(product_prices_field["group"], "previous.extract")

        product_prices_field = filter(lambda f: f["value"].endswith("extract_i_0"), fields)[2]
        self.assertTrue(product_prices_field["multiple"])
        self.assertEquals(product_prices_field["name"], "Diff Product Prices")
        self.assertEquals(product_prices_field["group"], "diff.extract")

        # Test enabled fields
        # As we generate 5 fields for each type (i, f, s, b) we hide all
        # fields that are not reserved
        filters = [f["value"] for f in filter(lambda f: f["value"].startswith("extract."), fields)]
        self.assertEquals(filters, ["extract.extract_i_0"])


class ParsingTestCase(unittest.TestCase):
    def setUp(self):
        self.parser = QueryParser(ElasticSearchBackend(ExtractResultsStreamDef.URL_DOCUMENT_MAPPING))

    def assertParsingError(self, func, *args, **kwargs):
        self.assertRaises(BotifyQueryException,
                          func, *args, **kwargs)


class TestExtractParsing(ParsingTestCase):
    def test_predicate_filter(self):
        f = self.parser.parse_predicate_filter({
                'field': 'extract.extract_i_0',
                'predicate': 'eq',
                'value': '179'
            })
        f.validate()
