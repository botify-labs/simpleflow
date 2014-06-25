import unittest
import tempfile
import gzip
import os
import StringIO

from cdf.features.sitemap.document import (open_sitemap_file,
                                           SiteMapType,
                                           SitemapXmlDocument,
                                           SitemapIndexXmlDocument,
                                           SitemapRssDocument,
                                           SitemapTextDocument,
                                           guess_sitemap_type)
from cdf.features.sitemap.exceptions import ParsingError


class TestSitemapXmlDocument(unittest.TestCase):
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
        sitemap_document = SitemapXmlDocument(self.file.name)
        self.assertEqual(SiteMapType.SITEMAP_XML,
                         sitemap_document.get_sitemap_type())
        self.assertEqual(["http://foo/bar"], list(sitemap_document.get_urls()))

    def test_sitemap_different_namespace(self):
        self.file.write('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.google.com/schemas/sitemap/0.84">'
                        '<url><loc>http://foo/bar</loc></url>'
                        '</urlset>')
        self.file.close()
        sitemap_document = SitemapXmlDocument(self.file.name)
        self.assertEqual(SiteMapType.SITEMAP_XML,
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
        sitemap_document = SitemapXmlDocument(self.file.name)
        self.assertEqual(SiteMapType.SITEMAP_XML,
                         sitemap_document.get_sitemap_type())
        self.assertEqual(["http://foo/bar/baz"],
                         list(sitemap_document.get_urls()))


    def test_xml_parsing_error(self):
        self.file.write('<urlset><url></url>')
        self.file.close()
        sitemap_document = SitemapXmlDocument(self.file.name)
        self.assertEqual(SiteMapType.SITEMAP_XML, sitemap_document.get_sitemap_type())
        self.assertRaises(
            ParsingError,
            list,
            sitemap_document.get_urls())

    def test_not_sitemap(self):
        self.file.write('<foo></foo>')  # valid xml but not a sitemap
        self.file.close()
        sitemap_document = SitemapXmlDocument(self.file.name)
        self.assertEqual(SiteMapType.SITEMAP_XML, sitemap_document.get_sitemap_type())
        self.assertEqual([], list(sitemap_document.get_urls()))


class TestSitemapIndexXmlDocument(unittest.TestCase):
    def setUp(self):
        self.file = tempfile.NamedTemporaryFile(delete=False)

    def tearDown(self):
        os.remove(self.file.name)

    def test_nominal_case(self):
        self.file.write('<?xml version="1.0" encoding="UTF-8"?>'
                        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                        '<sitemap><loc>http://foo/sitemap.xml.gz</loc></sitemap>'
                        '</sitemapindex>')
        self.file.close()
        sitemap_document = SitemapIndexXmlDocument(self.file.name)

        self.assertEqual(SiteMapType.SITEMAP_INDEX,
                         sitemap_document.get_sitemap_type())
        self.assertEqual(["http://foo/sitemap.xml.gz"],
                         list(sitemap_document.get_urls()))

    def test_no_namespace(self):
        self.file.write('<sitemapindex>'
                        '<sitemap><loc>http://foo.com/bar</loc></sitemap>'
                        '</sitemapindex>')
        self.file.close()
        sitemap_document = SitemapIndexXmlDocument(self.file.name)
        self.assertEqual(SiteMapType.SITEMAP_INDEX,
                         sitemap_document.get_sitemap_type())
        self.assertEqual(["http://foo.com/bar"],
                         list(sitemap_document.get_urls()))


class TestSitemapRssDocument(unittest.TestCase):
    def setUp(self):
        self.file = tempfile.NamedTemporaryFile(delete=False)

    def tearDown(self):
        os.remove(self.file.name)

    def test_nominal_case(self):
        self.file.write('<?xml version="1.0" encoding="UTF-8" ?>'
                         '<rss version="2.0">'
                         '<channel>'
                         ' <title>RSS Title</title>'
                         ' <description>This is an example of an RSS feed</description>'
                         ' <link>http://www.example.com/main.html</link>'
                         ' <item>'
                         '  <title>Example entry</title>'
                         '  <description>Here is some text containing an interesting description.</description>'
                         '  <link>http://www.example.com/blog/post/1</link>'
                         ' </item>'
                         '</channel>'
                         '</rss>')
        self.file.close()
        sitemap_document = SitemapRssDocument(self.file.name)
        self.assertEqual(SiteMapType.SITEMAP_RSS,
                         sitemap_document.get_sitemap_type())
        expected_urls = [
            "http://www.example.com/main.html",
            "http://www.example.com/blog/post/1"
        ]

        self.assertEqual(expected_urls, list(sitemap_document.get_urls()))


class TestSitemapTextDocument(unittest.TestCase):
    def setUp(self):
        self.file = tempfile.NamedTemporaryFile(delete=False)

    def tearDown(self):
        os.remove(self.file.name)

    def test_nominal_case(self):
        self.file.write('http://foo.com/bar\n'
                        'http://foo.com/baz\n'
                        'http://foo.com/qux\n')
        self.file.close()
        sitemap_document = SitemapTextDocument(self.file.name)
        self.assertEqual(SiteMapType.SITEMAP_TEXT,
                         sitemap_document.get_sitemap_type())
        expected_urls = [
            "http://foo.com/bar",
            "http://foo.com/baz",
            "http://foo.com/qux"
        ]

        self.assertEqual(expected_urls, list(sitemap_document.get_urls()))


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


class TestGuessSitemapDocumentType(unittest.TestCase):

    def test_xml_sitemap(self):
        file_mock = StringIO.StringIO('<?xml version="1.0" encoding="UTF-8"?>'
                                      '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                                      '<url><loc>http://foo/bar</loc></url>'
                                      '</urlset>')
        self.assertEqual(SiteMapType.SITEMAP_XML, guess_sitemap_type(file_mock))

    def test_xml_sitemapindex(self):
        file_mock = StringIO.StringIO('<?xml version="1.0" encoding="UTF-8"?>'
                                      '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                                      '<sitemap><loc>http://foo/sitemap.xml.gz</loc></sitemap>'
                                      '</sitemapindex>')
        self.assertEqual(SiteMapType.SITEMAP_INDEX,
                         guess_sitemap_type(file_mock))

    def test_rss_sitemap(self):
        file_mock = StringIO.StringIO('<?xml version="1.0" encoding="UTF-8" ?>'
                                      '<rss version="2.0">'
                                      '<channel>'
                                      ' <title>RSS Title</title>'
                                      ' <description>This is an example of an RSS feed</description>'
                                      ' <link>http://www.example.com/main.html</link>'
                                      ' <item>'
                                      '  <title>Example entry</title>'
                                      '  <description>Here is some text containing an interesting description.</description>'
                                      '  <link>http://www.example.com/blog/post/1</link>'
                                      ' </item>'
                                      '</channel>'
                                      '</rss>')
        self.assertEqual(SiteMapType.SITEMAP_RSS,
                         guess_sitemap_type(file_mock))

    def test_xml_syntax_error(self):
        file_mock = StringIO.StringIO('<foo></bar>')
        self.assertEqual(SiteMapType.UNKNOWN, guess_sitemap_type(file_mock))

    def test_simple_xml(self):
        file_mock = StringIO.StringIO('<foo></foo>')
        self.assertEqual(SiteMapType.UNKNOWN, guess_sitemap_type(file_mock))
