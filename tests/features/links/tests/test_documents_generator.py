# -*- coding:utf-8 -*-
"""Unit test for url document generation
Source stream format reminder:
    - urlids: uid, protocol, host, path
    - urlinfos: uid, mask, content type, depth, date,
        http code, size, delay_first_byte, delay_last_byte
    - urllinks: src uid, link type, follow mask, dest uid
    - urlinlinks: dest uid, link type, follow mask, src uid
    - urlcontents: uid, content type code, hash.fnv64.unsigned, content
    - content_duplicate: uid, content_type, filled number,
        duplicate number, is_first, duplicating url list
"""

import unittest
import logging

from cdf.log import logger
from cdf.analysis.urls.generators.documents import UrlDocumentGenerator
from cdf.features.links.helpers.masks import follow_mask
from cdf.features.main.streams import IdStreamDef, InfosStreamDef
from cdf.features.links.streams import InlinksStreamDef


logger.setLevel(logging.DEBUG)


def _next_doc(generator):
    return next(generator)[1]


class TestBasicInfoGeneration(unittest.TestCase):

    def setUp(self):
        self.patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        self.infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        ]

    def test_top_anchors(self):
        inlinks = [
            [1, 'a', 0, 2, "12D", "Yeah"],
            [1, 'r301', 0, 3, None, None],
            [1, 'a', 0, 3, "12D", "\x00"],
            [1, 'a', 0, 4, "13D", "Oops"],
            [1, 'a', 0, 4, "13D", "\x00"],
            [1, 'a', 0, 4, "12D", "\x00"],
            [1, 'a', 0, 4, "14D", ""],  # Empty text
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.get_stream_from_iterator(iter(self.patterns)),
            InfosStreamDef.get_stream_from_iterator(iter(self.infos)),
            InlinksStreamDef.get_stream_from_iterator(iter(inlinks)),
        ])

        document = _next_doc(gen)
        self.assertEquals(document["inlinks_internal"]["anchors"]["nb"], 3)
        self.assertEquals(
            document["inlinks_internal"]["anchors"]["top"]['text'],
            ["Yeah", "Oops", "[empty]"]
        )
        self.assertEquals(
            document["inlinks_internal"]["anchors"]["top"]['nb'],
            [3, 2, 1]
        )

    def test_top_anchors_empty_text_canonical(self):
        # Empty text is in the canonical line,
        # Which is not analyzed
        inlinks = [
            [1, 'canonical', 0, 3, "0", ""],
            [1, 'a', 0, 3, "0", "\x00"],
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.get_stream_from_iterator(iter(self.patterns)),
            InfosStreamDef.get_stream_from_iterator(iter(self.infos)),
            InlinksStreamDef.get_stream_from_iterator(iter(inlinks)),
        ])

        document = _next_doc(gen)
        self.assertEquals(
            document["inlinks_internal"]["anchors"]["top"]['text'],
            ["[empty]"]
        )
        self.assertEquals(
            document["inlinks_internal"]["anchors"]["top"]['nb'],
            [1]
        )

    def test_top_anchors_not_set(self):
        inlinks = [
            [1, 'a', 0, 2],
            [1, 'r301', 0, 3],
            [1, 'a', 0, 3],
            [1, 'a', 0, 4],
            [1, 'a', 0, 4],
            [1, 'a', 0, 4],
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.get_stream_from_iterator(iter(self.patterns)),
            InfosStreamDef.get_stream_from_iterator(iter(self.infos)),
            InlinksStreamDef.get_stream_from_iterator(iter(inlinks)),
        ])

        document = _next_doc(gen)
        self.assertTrue("nb" not in document["inlinks_internal"]["anchors"]["top"])
