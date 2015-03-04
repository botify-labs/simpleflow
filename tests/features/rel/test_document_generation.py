import shutil
import tempfile
import unittest

from cdf.compat import json
from cdf.analysis.urls.generators.documents import UrlDocumentGenerator

from cdf.features.main.streams import (
    CompliantUrlStreamDef,
    IdStreamDef, InfosStreamDef
)
from cdf.features.rel.streams import RelCompliantStreamDef, InRelStreamDef
from cdf.features.rel import constants as rel_constants

import boto
from moto import mock_s3

from bitarray import bitarray


def _next_doc(generator):
    return next(generator)[1]


class TestRelDocument(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.s3_uri = 's3://test_bucket/analysis'

        self.patterns = [
            [1, 'http', 'www.site.com', '/1', ''],
            [2, 'http', 'www.site.com', '/2', ''],
            [3, 'http', 'www.site.com', '/3', ''],
        ]

        self.infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456, "en"],
        ]

        self.compliant = [
            [1, 'true', 0],
            [2, 'true', 0],
            [3, 'false', 0],
            [4, 'false', 1],
        ]

        # Rel stream format
        # uid_from type mask uid_to url value uid_to_is_compliant
        # type :
        # 1 = hreflang
        # 2 = prev
        # 3 = next
        # 4 = author
        self.rel = [
            [1, 1, 0, 2, "", "en-US", "1"], # OK
            [1, 1, 0, 2, "", "x-default", "1"], # OK, x-default value
            [1, 1, 0, -1, "http://www.site.com/it", "it-IT", ""], # OK but warning to external URL
            [1, 1, 0, 3, "", "jj-us", "0"], # KO : Bad Lang
            [1, 1, 0, 3, "", "en-ZZ", "0"], # KO : Bad Country
            [1, 1, 4, -1, "http://www.site.com/blocked-robot-txt", "en-US", ""], # OK but warning : Blocked by robotstxt
            [1, 1, 8, -1, "http://www.site.com/blocked-config", "en-US", ""], # OK but warning : Blocked by config
        ]

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    @mock_s3
    def test_hreflang_out(self):
        conn = boto.connect_s3()
        bucket = conn.create_bucket('test_bucket')

        gen = UrlDocumentGenerator(
            [
                IdStreamDef.load_iterator(iter(self.patterns)),
                InfosStreamDef.load_iterator(iter(self.infos)),
                CompliantUrlStreamDef.load_iterator(iter(self.compliant)),
                RelCompliantStreamDef.load_iterator(iter(self.rel))
            ]
        )

        document = _next_doc(gen)
        href = document["rel"]["hreflang"]["out"]
        self.assertEquals(href["nb"], 7)

        # Valid
        self.assertEquals(href["valid"]["nb"], 5)
        self.assertTrue(href["valid"]["sends_x-default"])
        self.assertEquals(href["valid"]["langs"], ["en", "it"])
        self.assertEquals(href["valid"]["regions"], ["en-US", "it-IT"])
        self.assertEquals(
                href["valid"]["warning"],
                [rel_constants.WARNING_DEST_BLOCKED_CONFIG,
                rel_constants.WARNING_DEST_NOT_CRAWLED,
                rel_constants.WARNING_DEST_BLOCKED_ROBOTS_TXT])

        # Errors
        self.assertEquals(href["not_valid"]["nb"], 2)
        self.assertEquals(
                href["not_valid"]["errors"],
                [rel_constants.ERROR_COUNTRY_NOT_RECOGNIZED,
                 rel_constants.ERROR_LANG_NOT_RECOGNIZED,
                 rel_constants.ERROR_DEST_NOT_COMPLIANT]
        )

        # Samples are JSON serialized objects
        # (We don't want to store them as objects)

        # Success
        samples = json.loads(href["valid"]["values"])
        self.assertEquals(len(samples), 5)
        self.assertEquals(
                samples[0],
                {u"url_id": 2,
                 u"value": u"en-US",
                 u"warning": []}
        )
        self.assertEquals(
                samples[1],
                {u"url_id": 2,
                 u"value": u"x-default",
                 u"warning": []}
        )
        self.assertEquals(
                samples[2],
                {u"url": "http://www.site.com/it",
                 u"value": u"it-IT",
                 u"warning": [rel_constants.WARNING_DEST_NOT_CRAWLED]}
        )

        self.assertEquals(
                samples[3],
                {u"url": "http://www.site.com/blocked-robot-txt",
                 u"value": u"en-US",
                 u"warning": [rel_constants.WARNING_DEST_BLOCKED_ROBOTS_TXT]}
        )
        self.assertEquals(
                samples[4],
                {u"url": "http://www.site.com/blocked-config",
                 u"value": u"en-US",
                 u"warning": [rel_constants.WARNING_DEST_BLOCKED_CONFIG]}
        )

        # Error samples
        samples = json.loads(href["not_valid"]["values"])
        self.assertEquals(len(samples), 2)
        self.assertEquals(
                samples[0],
                {u"url_id": 3,
                 u"errors": [rel_constants.ERROR_LANG_NOT_RECOGNIZED,
                             rel_constants.ERROR_DEST_NOT_COMPLIANT],
                 u"value": u"jj-us"}
        )
        self.assertEquals(
                samples[1],
                {u"url_id": 3,
                    u"errors": [rel_constants.ERROR_COUNTRY_NOT_RECOGNIZED,
                                rel_constants.ERROR_DEST_NOT_COMPLIANT],
                 u"value": u"en-ZZ"}
        )


class TestInRelDocument(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.s3_uri = 's3://test_bucket/analysis'

        self.patterns = [
            [1, 'http', 'www.site.com', '/1', ''],
            [2, 'http', 'www.site.com', '/2', ''],
            [3, 'http', 'www.site.com', '/3', ''],
        ]

        self.infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456, "en"], # lang en
            [2, 1, 'text/html', 0, 1, 200, 1200, 303, 456, "en"], # lang en
            [3, 1, 'text/html', 0, 1, 200, 1200, 303, 456, "en"], # lang en
        ]

        self.compliant = [
            [1, 'true', 0],
            [2, 'true', 0],
            [3, 'false', 0],
        ]

        # Rel stream format
        # uid_to type mask uid_from value
        # type :
        # 1 = hreflang
        # 2 = prev
        # 3 = next
        # 4 = author
        self.rel = [
            [1, 1, 0, 2, "en-US"], # OK
            [1, 1, 0, 2, "x-default"], # Default page
            [1, 1, 0, 3, "en-US"], # OK
            [2, 1, 0, 3, "zz"], # Bad Lang
            [3, 1, 0, 2, "en-US"], # Lang OK
            [3, 1, 0, 2, "fr-FR"], # Lang not equal
        ]

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    @mock_s3
    def test_hreflang_in(self):
        conn = boto.connect_s3()
        bucket = conn.create_bucket('test_bucket')

        gen = UrlDocumentGenerator(
            [
                IdStreamDef.load_iterator(iter(self.patterns)),
                InfosStreamDef.load_iterator(iter(self.infos)),
                CompliantUrlStreamDef.load_iterator(iter(self.compliant)),
                InRelStreamDef.load_iterator(iter(self.rel))
            ]
        )

        # URL 1
        document = _next_doc(gen)
        href = document["rel"]["hreflang"]["in"]
        self.assertEquals(href["nb"], 3)

        self.assertEquals(href["valid"]["nb"], 3)
        self.assertEquals(href["valid"]["langs"], ["en"])
        self.assertEquals(href["valid"]["regions"], ["en-US"])
        self.assertTrue(href["receives_x-default"])
        self.assertEquals(json.loads(href["valid"]["values"]),
                          [
                            {"url_id": 2, "value": "en-US"},
                            {"url_id": 2, "value": "x-default"},
                            {"url_id": 3, "value": "en-US"}
                          ]
        )
        self.assertEquals(href["not_valid"]["nb"], 0)
        # Errors is cleaned
        self.assertTrue("errors" not in href["not_valid"])
        self.assertEquals(href["not_valid"]["values"], '[]')

        # URL 2
        document = _next_doc(gen)
        href = document["rel"]["hreflang"]["in"]
        self.assertEquals(href["nb"], 1)
        self.assertEquals(href["valid"]["nb"], 0)
        self.assertEquals(href["not_valid"]["nb"], 1)
        self.assertEquals(
                json.loads(href["not_valid"]["values"]),
                [{"url_id": 3, "value": "zz", "errors": [rel_constants.ERROR_LANG_NOT_RECOGNIZED]}]
        )

        # URL 3
        document = _next_doc(gen)
        href = document["rel"]["hreflang"]["in"]
        self.assertEquals(href["nb"], 2)
        self.assertEquals(href["valid"]["nb"], 1)
        self.assertEquals(
                json.loads(href["valid"]["values"]),
                [{"url_id": 2, "value": "en-US"}]
        )
        self.assertEquals(href["not_valid"]["nb"], 1)
        self.assertEquals(
                json.loads(href["not_valid"]["values"]),
                [{"url_id": 2, "value": "fr-FR", "errors": [rel_constants.ERROR_LANG_NOT_EQUAL]}]
        )
