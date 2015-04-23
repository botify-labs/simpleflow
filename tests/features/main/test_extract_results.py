# -*- coding: utf-8 -*-
__author__ = 'zeb'

import unittest

from cdf.tasks.documents import UrlDocumentGenerator
from cdf.features.main.streams import (
    IdStreamDef,
    ExtractResultsStreamDef
)


_stream1 = [
    # docid, name, label, agg, cast, rank, value
    (1, 'img-ALT', "extract_string_0", "list", "s", 1, ""),
    (1, 'img-ALT', "extract_string_0", "list", "s", 2, ""),
    (1, "html-truncate", "extract_string_1", "first", "", 0, r'<html class="ie"><![endif]-->\n<!--[if IE 9]><html class="ie9"><![endif]-->\n<!--'),
    (1, "titleFULL", "extract_string_2", "first", "", 0, r'<title>Le Monde.fr - Actualité à la Une</title>'),
    (1, "count", "extract_int_0", "count", "i", 0, 179),
    (1, 'img-ALT', "extract_string_0", "list", "s", 0, "Avatar lemonde.fr"),
    (2, 'img-ALT', "extract_string_0", "list", "s", 1, ""),
    (2, 'img-ALT', "extract_string_0", "list", "s", 2, ""),
    (2, "html-truncate", "extract_string_1", "first", "", 0, r'<html class="ie"><![endif]-->\n<!--[if IE 9]><html class="ie9"><![endif]-->\n<!--'),
    (2, "titleFULL", "extract_string_2", "first", "", 0, r'<title>Le Monde.fr - Actualité à la Une</title>'),
    (2, "count", "extract_int_0", "count", "i", 0, 5),
    (2, 'img-ALT', "extract_string_0", "list", "s", 0, ""),
]

_ids1 = [
    [1, 'http', 'www.site.com', '/path/name.html', ''],
    [2, 'http', 'www.site.com', '/path/other_name.html', ''],
]


class TestExtractResultsStreamDef(unittest.TestCase):
    def test1(self):
        gen = UrlDocumentGenerator(
            [IdStreamDef.load_iterator(iter(_ids1)), ExtractResultsStreamDef.load_iterator(iter(_stream1))])
        documents = list(gen)
        self.assertEqual(len(documents), 2)
        doc1 = documents[0][1]
        self.assertIn("extract", doc1)
        extract_ = doc1["extract"]
        self.assertIn("extract_string_0", extract_)
        self.assertEqual(extract_["extract_string_0"], ["Avatar lemonde.fr", "", ""])
        self.assertEqual(extract_["extract_string_2"], "<title>Le Monde.fr - Actualité à la Une</title>")
        self.assertEqual(extract_["extract_int_0"], 179)

    def test_rank(self):
        s = (1, "toto", "extract_string_0", "list", "s", 9, "thing")
        doc = {"extract": {"extract_string_0": None}}
        sd = ExtractResultsStreamDef()
        sd.process_document(doc, s)
        self.assertEqual(10, len(doc["extract"]["extract_string_0"]))
        self.assertEqual("thing", doc["extract"]["extract_string_0"][9])
