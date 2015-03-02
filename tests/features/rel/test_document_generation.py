import unittest

from cdf.compat import json
from cdf.analysis.urls.generators.documents import UrlDocumentGenerator

from cdf.features.main.streams import (
    CompliantUrlStreamDef,
    IdStreamDef, InfosStreamDef
)
from cdf.features.rel.streams import RelStreamDef
from cdf.features.rel import constants as rel_constants


def _next_doc(generator):
    return next(generator)[1]


class TestRelDocument(unittest.TestCase):

    def setUp(self):
        self.patterns = [
            [1, 'http', 'www.site.com', '/1', ''],
            [2, 'http', 'www.site.com', '/2', ''],
            [3, 'http', 'www.site.com', '/3', ''],
        ]

        self.infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        ]

        self.compliant = [
            [1, 'true', 0],
            [2, 'true', 0],
            [3, 'true', 0],
        ]

        # Rel stream format
        # uid_from type mask uid_to url value
        # type :
        # 1 = hreflang
        # 2 = prev
        # 3 = next
        # 4 = author
        self.rel = [
            [1, 1, 0, 2, "", "en-US"], # OK
            [1, 1, 0, -1, "http://www.site.com/it", "it-IT"], # OK but warning to external URL
            [1, 1, 0, 3, "", "jj-us"], # KO : Bad Lang
            [1, 1, 0, 3, "", "en-ZZ"], # KO : Bad Country
            [1, 1, 4, -1, "http://www.site.com/blocked-robot-txt", "en-US"], # OK but warning : Blocked by robotstxt
            [1, 1, 8, -1, "http://www.site.com/blocked-config", "en-US"], # OK but warning : Blocked by config
        ]

    def test_hreflang_out(self):
        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(self.patterns)),
            InfosStreamDef.load_iterator(iter(self.infos)),
            CompliantUrlStreamDef.load_iterator(iter(self.compliant)),
            RelStreamDef.load_iterator(iter(self.rel))
        ])

        document = _next_doc(gen)
        href = document["rel"]["hreflang"]["out"]
        self.assertEquals(href["nb"], 6)

        # Valid
        self.assertEquals(href["valid"]["nb"], 4)
        self.assertEquals(href["valid"]["langs"], ["en-US", "it-IT"])
        self.assertEquals(
                href["valid"]["warning"],
                [rel_constants.WARNING_DEST_BLOCKED_CONFIG,
                rel_constants.WARNING_DEST_NOT_CRAWLED,
                rel_constants.WARNING_DEST_BLOCKED_ROBOTS_TXT])

        # Errors
        self.assertEquals(href["not_valid"]["nb"], 2)
        self.assertEquals(
                href["not_valid"]["errors"],
                [rel_constants.ERROR_COUNTRY_NOT_RECOGNIZED, rel_constants.ERROR_LANG_NOT_RECOGNIZED]
        )

        # Samples are JSON serialized objects
        # (We don't want to store them as objects)

        # Success
        samples = [json.loads(k) for k in href["valid"]["samples"]]
        self.assertEquals(len(samples), 4)
        self.assertEquals(
                samples[0],
                {u"url_id": 2,
                 u"lang": u"en-US",
                 u"warning": []}
        )
        self.assertEquals(
                samples[1],
                {u"url": "http://www.site.com/it",
                 u"lang": u"it-IT",
                 u"warning": [rel_constants.WARNING_DEST_NOT_CRAWLED]}
        )

        self.assertEquals(
                samples[2],
                {u"url": "http://www.site.com/blocked-robot-txt",
                 u"lang": u"en-US",
                 u"warning": [rel_constants.WARNING_DEST_BLOCKED_ROBOTS_TXT]}
        )
        self.assertEquals(
                samples[3],
                {u"url": "http://www.site.com/blocked-config",
                 u"lang": u"en-US",
                 u"warning": [rel_constants.WARNING_DEST_BLOCKED_CONFIG]}
        )

        # Error samples
        samples = [json.loads(k) for k in href["not_valid"]["samples"]]
        self.assertEquals(len(samples), 2)
        self.assertEquals(
                samples[0],
                {u"url_id": 3,
                 u"errors": [rel_constants.ERROR_LANG_NOT_RECOGNIZED],
                 u"value": u"jj-us"}
        )
        self.assertEquals(
                samples[1],
                {u"url_id": 3,
                 u"errors": [rel_constants.ERROR_COUNTRY_NOT_RECOGNIZED],
                 u"value": u"en-ZZ"}
        )
