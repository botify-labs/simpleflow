import unittest
import tempfile
import gzip
import os

from cdf.features.sitemap.document import (open_sitemap_file,
                                           SiteMapType,
                                           SitemapDocument)
from cdf.features.sitemap.exceptions import ParsingError


class TestSitemapDocument(unittest.TestCase):
    def setUp(self):
        self.file = tempfile.NamedTemporaryFile(delete=False)

    def tearDown(self):
        os.remove(self.file.name)

    def test_sitemap_0_9(self):
        self.file.write('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                        '<url><loc>http://foo/bar</loc></url>'
                        '</urlset>')
        self.file.close()
        sitemap_document = SitemapDocument(self.file.name)
        self.assertEqual(SiteMapType.SITEMAP,
                         sitemap_document.get_sitemap_type())
        self.assertEqual(["http://foo/bar"], list(sitemap_document.get_urls()))

    def test_sitemap_index_0_9(self):
        self.file.write('<?xml version="1.0" encoding="UTF-8"?>'
                        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                        '<sitemap><loc>http://foo/sitemap.xml.gz</loc></sitemap>'
                        '</sitemapindex>')
        self.file.close()
        sitemap_document = SitemapDocument(self.file.name)
        self.assertEqual(SiteMapType.SITEMAP_INDEX,
                         sitemap_document.get_sitemap_type())
        self.assertEqual(["http://foo/sitemap.xml.gz"],
                         list(sitemap_document.get_urls()))

    def test_sitemap_different_namespace(self):
        self.file.write('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.google.com/schemas/sitemap/0.84">'
                        '<url><loc>http://foo/bar</loc></url>'
                        '</urlset>')
        self.file.close()
        sitemap_document = SitemapDocument(self.file.name)
        self.assertEqual(SiteMapType.SITEMAP,
                         sitemap_document.get_sitemap_type())
        self.assertEqual(["http://foo/bar"],
                         list(sitemap_document.get_urls()))

    def test_sitemap_multiple_namespaces(self):
        self.file.write('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" '
                        'xmlns:mobile="http://www.google.com/schemas/sitemap-mobile/1.0" '
                        'xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">'
                        '  <url>'
                        '<loc>http://foo/bar/baz</loc>'
                        '  </url>'
                        '</urlset>')
        self.file.close()
        sitemap_document = SitemapDocument(self.file.name)
        self.assertEqual(SiteMapType.SITEMAP,
                         sitemap_document.get_sitemap_type())
        self.assertEqual(["http://foo/bar/baz"],
                         list(sitemap_document.get_urls()))

    def test_sitemap_no_namespace(self):
        self.file.write('<sitemapindex>'
                        '<sitemap><loc>http://foo.com/bar</loc></sitemap>'
                        '</sitemapindex>')
        self.file.close()
        sitemap_document = SitemapDocument(self.file.name)
        self.assertEqual(SiteMapType.SITEMAP_INDEX,
                         sitemap_document.get_sitemap_type())
        self.assertEqual(["http://foo.com/bar"],
                         list(sitemap_document.get_urls()))

    def test_xml_parsing_error(self):
        self.file.write('<urlset><url></url>')
        self.file.close()
        sitemap_document = SitemapDocument(self.file.name)
        self.assertEqual(SiteMapType.SITEMAP, sitemap_document.get_sitemap_type())
        self.assertRaises(
            ParsingError,
            list,
            sitemap_document.get_urls())

    def test_not_sitemap(self):
        self.file.write('<foo></foo>')  # valid xml but not a sitemap
        self.file.close()
        sitemap_document = SitemapDocument(self.file.name)
        self.assertEqual(SiteMapType.UNKNOWN, sitemap_document.get_sitemap_type())
        self.assertEqual([], list(sitemap_document.get_urls()))


class TestOpenSitemapFile(unittest.TestCase):
    def setUp(self):
        self.file = tempfile.NamedTemporaryFile(delete=False)
        self.file_path = self.file.name
        self.file.close()

    def tearDown(self):
        os.remove(self.file_path)

    def test_text_file(self):
        with open(self.file_path, "w") as f:
            f.write("foo bar")

        with open_sitemap_file(self.file_path) as f:
            self.assertEqual("foo bar", f.read())

    def test_gzip_file(self):
        with gzip.open(self.file_path, "w") as f:
            f.write("foo bar")

        with open_sitemap_file(self.file_path) as f:
            self.assertEqual("foo bar", f.read())
