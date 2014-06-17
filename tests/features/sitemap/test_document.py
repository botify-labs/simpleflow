import unittest
import StringIO
import tempfile
import gzip
import os

from cdf.features.sitemap.document import (open_sitemap_file,
                                           guess_sitemap_type,
                                           get_urls,
                                           SiteMapType)
from cdf.features.sitemap.exceptions import ParsingError


class TestGuessSiteMapType(unittest.TestCase):
    def test_sitemap_0_9(self):
        file_content = ('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                        '<url><loc>http://foo/bar</loc></url>'
                        '</urlset>')
        actual_result = guess_sitemap_type(StringIO.StringIO(file_content))
        self.assertEqual(SiteMapType.SITEMAP, actual_result)

    def test_sitemap_index_0_9(self):
        file_content = ('<?xml version="1.0" encoding="UTF-8"?>'
                        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                        '<sitemap><loc>http://foo/sitemap.xml.gz</loc></sitemap>'
                        '</sitemapindex>')
        actual_result = guess_sitemap_type(StringIO.StringIO(file_content))
        self.assertEqual(SiteMapType.SITEMAP_INDEX, actual_result)

    def test_sitemap_different_namespace(self):
        file_content = ('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.google.com/schemas/sitemap/0.84">'
                        '<url><loc>http://foo/bar</loc></url>'
                        '</urlset>')
        actual_result = guess_sitemap_type(StringIO.StringIO(file_content))
        self.assertEqual(SiteMapType.SITEMAP, actual_result)

    def test_sitemap_multiple_namespaces(self):
        file_content = ('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" '
                        'xmlns:mobile="http://www.google.com/schemas/sitemap-mobile/1.0" '
                        'xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">'
                        '  <url>'
                        '<loc>http://foo/bar/baz</loc>'
                        '  </url>'
                        '</urlset>')
        actual_result = guess_sitemap_type(StringIO.StringIO(file_content))
        self.assertEqual(SiteMapType.SITEMAP, actual_result)

    def test_sitemap_no_namespace(self):
        file_content = ('<sitemapindex>'
                        '<sitemap><loc>http://foo.com/bar</loc></sitemap>'
                        '</sitemapindex>')
        actual_result = guess_sitemap_type(StringIO.StringIO(file_content))
        self.assertEqual(SiteMapType.SITEMAP_INDEX, actual_result)

    def test_xml_parsing_error(self):
        file_content = '<urlset><url></url>'
        actual_result = guess_sitemap_type(StringIO.StringIO(file_content))
        self.assertEqual(SiteMapType.UNKNOWN, actual_result)

    def test_not_sitemap(self):
        file_content = '<foo></foo>'  # valid xml but not a sitemap
        actual_result = guess_sitemap_type(StringIO.StringIO(file_content))
        self.assertEqual(SiteMapType.UNKNOWN, actual_result)


class TestGetUrls(unittest.TestCase):
    def test_sitemap_0_9(self):
        file_content = ('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                        '<url><loc>http://foo/bar</loc></url>'
                        '</urlset>')
        actual_result = get_urls(StringIO.StringIO(file_content))
        self.assertEqual(["http://foo/bar"], list(actual_result))

    def test_sitemap_index_0_9(self):
        file_content = ('<?xml version="1.0" encoding="UTF-8"?>'
                        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                        '<sitemap><loc>http://foo/sitemap.xml.gz</loc></sitemap>'
                        '</sitemapindex>')
        actual_result = get_urls(StringIO.StringIO(file_content))
        self.assertEqual(["http://foo/sitemap.xml.gz"], list(actual_result))


    def test_sitemap_different_namespace(self):
        file_content = ('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.google.com/schemas/sitemap/0.84">'
                        '<url><loc>http://foo/bar</loc></url>'
                        '</urlset>')
        actual_result = get_urls(StringIO.StringIO(file_content))
        self.assertEqual(["http://foo/bar"], list(actual_result))


    def test_sitemap_multiple_namespaces(self):
        file_content = ('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:mobile="http://www.google.com/schemas/sitemap-mobile/1.0" xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">'
                        '  <url>'
                        '<loc>http://foo/bar/baz</loc>'
                        '  </url>'
                        '</urlset>')
        actual_result = get_urls(StringIO.StringIO(file_content))
        self.assertEqual(["http://foo/bar/baz"], list(actual_result))

    def test_sitemap_no_namespace(self):
        file_content = ('<sitemapindex>'
                        '<sitemap><loc>http://foo.com/bar</loc></sitemap>'
                        '</sitemapindex>')
        actual_result = get_urls(StringIO.StringIO(file_content))
        self.assertEqual(["http://foo.com/bar"], list(actual_result))

    def test_xml_parsing_error(self):
        file_content = '<urlset><url></url>'
        generator = get_urls(StringIO.StringIO(file_content))

        self.assertRaises(ParsingError,
                          list,
                          generator)

    def test_not_sitemap(self):
        file_content = '<foo></foo>'  # valid xml but not a sitemap
        actual_result = get_urls(StringIO.StringIO(file_content))
        self.assertEquals(0, len(list(actual_result)))


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
