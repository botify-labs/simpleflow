import unittest
from cdf.analysis.urls.generators.documents import UrlDocumentGenerator

from cdf.features.main.streams import (
    CompliantUrlStreamDef,
    IdStreamDef, InfosStreamDef
)


def _next_doc(generator):
    return next(generator)[1]


class TestStrategicUrlDocument(unittest.TestCase):

    def setUp(self):
        self.patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        self.infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        ]

    def test_strategic(self):
        strategic = [
            [1, 'true', 0],
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(self.patterns)),
            InfosStreamDef.load_iterator(iter(self.infos)),
            CompliantUrlStreamDef.load_iterator(iter(strategic))
        ])

        document = _next_doc(gen)
        self.assertTrue(document['strategic']['is_strategic'])
        self.assertEqual(document['strategic']['reason'], {})

    def test_non_strategic(self):
        strategic = [
            [1, 'false', 4],
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(self.patterns)),
            InfosStreamDef.load_iterator(iter(self.infos)),
            CompliantUrlStreamDef.load_iterator(iter(strategic))
        ])

        document = _next_doc(gen)
        reason = document['strategic']['reason']

        self.assertEqual(document['strategic']['is_strategic'], False)
        self.assertEqual(reason['content_type'], True)
        self.assertEqual(reason['http_code'], False)
        self.assertEqual(reason['canonical'], False)
        self.assertEqual(reason['noindex'], False)