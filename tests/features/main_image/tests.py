import unittest

from cdf.analysis.urls.generators.documents import UrlDocumentGenerator
from cdf.features.main.streams import IdStreamDef, InfosStreamDef
from cdf.features.main_image.streams import ContentsExtendedStreamDef


class TestBasicInfoGeneration(unittest.TestCase):

    def setUp(self):
        self.patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        self.infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        ]

    def test_main_image(self):
        contents = [
            [1, 'm.prop', 1, 'og:type', 'article'],
            [1, 'm.prop', 2, 'og:image', 'http://www.site.com/image.jpg'],
            [1, 'm.prop', 3, 'og:image', 'http://www.site.com/another_image.jpg']
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.get_stream_from_iterator(iter(self.patterns)),
            InfosStreamDef.get_stream_from_iterator(iter(self.infos)),
            ContentsExtendedStreamDef.get_stream_from_iterator(iter(contents)),
        ])

        document = gen.next()[1]
        self.assertEquals(
            document["main_image"],
            'http://www.site.com/image.jpg'
        )

    def test_main_image_type_position(self):
        contents = [
            [1, 'm.prop', 1, 'og:type', 'article'],
            [1, 'm.prop', 3, 'og:image', 'http://www.site.com/image.jpg'],
            [1, 'm.prop', 2, 'og:image', 'http://www.site.com/another_image.jpg']
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.get_stream_from_iterator(iter(self.patterns)),
            InfosStreamDef.get_stream_from_iterator(iter(self.infos)),
            ContentsExtendedStreamDef.get_stream_from_iterator(iter(contents)),
        ])

        # We want image with position 2
        document = gen.next()[1]
        self.assertEquals(
            document["main_image"],
            'http://www.site.com/another_image.jpg'
        )

    def test_main_image_type_priority(self):
        contents = [
            [1, 'm.prop', 1, 'og:type', 'article'],
            [1, 'm.prop', 2, 'twitter:image', 'http://www.site.com/image.jpg'],
            [1, 'm.prop', 3, 'og:image', 'http://www.site.com/another_image.jpg']
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.get_stream_from_iterator(iter(self.patterns)),
            InfosStreamDef.get_stream_from_iterator(iter(self.infos)),
            ContentsExtendedStreamDef.get_stream_from_iterator(iter(contents)),
        ])

        # We want image with og:image (prior to twitter:image) despite a small position
        document = gen.next()[1]
        self.assertEquals(
            document["main_image"],
            'http://www.site.com/another_image.jpg'
        )
