import unittest
import mock
import tempfile
import gzip
import os
import csv

from cdf.features.sitemaps.document import (instanciate_sitemap_document,
                                           open_sitemap_file,
                                           SiteMapType,
                                           SitemapXmlDocument,
                                           SitemapIndexXmlDocument,
                                           SitemapRssDocument,
                                           SitemapTextDocument,
                                           UrlValidator,
                                           guess_sitemap_type,
                                           SitemapUrlValidator)
from cdf.features.sitemaps.exceptions import ParsingError, UnhandledFileType

class TestSitemapDocument(unittest.TestCase):
    def test_to_dict_nominal_case(self):
        document = SitemapXmlDocument("foo", "http://foo")
        document.valid_urls = 2
        document.invalid_urls = 1

        actual_result = document.to_dict()

        expected_result = {
            "type": "SITEMAP_XML",
            "valid": 2,
            "invalid": 1
        }
        self.assertEqual(expected_result, actual_result)

    def test_to_dict_error_case(self):
        document = SitemapXmlDocument("foo", "http://foo")
        document.valid_urls = 2
        document.invalid_urls = 1
        document.set_error("error", "error_message")
        actual_result = document.to_dict()

        expected_result = {
            "type": "SITEMAP_XML",
            "error": "error",
            "message": "error_message",
            "valid": 2,
            "invalid": 1
        }

        self.assertEqual(expected_result, actual_result)


class TestSitemapXmlDocument(unittest.TestCase):
    def setUp(self):
        self.file = tempfile.NamedTemporaryFile(delete=False)
        self.url = "http://foo/sitemap.xml"

    def tearDown(self):
        os.remove(self.file.name)

    def test_sitemap_0_9(self):
        self.file.write('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                        '<url><loc>http://foo/bar</loc></url>'
                        '</urlset>')
        self.file.close()
        sitemap_document = SitemapXmlDocument(self.file.name, self.url)
        self.assertEqual(SiteMapType.SITEMAP_XML,
                         sitemap_document.get_sitemap_type())
        self.assertEqual(["http://foo/bar"], list(sitemap_document.get_urls()))
        self.assertEqual(1, sitemap_document.valid_urls)
        self.assertEqual(0, sitemap_document.invalid_urls)


    def test_sitemap_different_namespace(self):
        self.file.write('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.google.com/schemas/sitemap/0.84">'
                        '<url><loc>http://foo/bar</loc></url>'
                        '</urlset>')
        self.file.close()
        sitemap_document = SitemapXmlDocument(self.file.name, self.url)
        self.assertEqual(SiteMapType.SITEMAP_XML,
                         sitemap_document.get_sitemap_type())
        self.assertEqual(["http://foo/bar"],
                         list(sitemap_document.get_urls()))
        self.assertEqual(1, sitemap_document.valid_urls)
        self.assertEqual(0, sitemap_document.invalid_urls)


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
        sitemap_document = SitemapXmlDocument(self.file.name, self.url)
        self.assertEqual(SiteMapType.SITEMAP_XML,
                         sitemap_document.get_sitemap_type())
        self.assertEqual(["http://foo/bar/baz"],
                         list(sitemap_document.get_urls()))
        self.assertEqual(1, sitemap_document.valid_urls)
        self.assertEqual(0, sitemap_document.invalid_urls)

    def test_invalid_url(self):
        self.file.write('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                        '<url><loc>http://foo/bar</loc></url>'
                        '<url><loc>foo</loc></url>'
                        '<url><loc>http://foo/baz</loc></url>'
                        '</urlset>')
        self.file.close()
        sitemap_document = SitemapXmlDocument(self.file.name, self.url)
        self.assertEqual(["http://foo/bar", "http://foo/baz"], list(sitemap_document.get_urls()))
        self.assertEqual(2, sitemap_document.valid_urls)
        self.assertEqual(1, sitemap_document.invalid_urls)

    def test_image_sitemap(self):
        self.file.write('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
                        ' xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">'
                        '<url>'
                        '    <loc>http://foo/bar</loc>'
                        '    <image:image>'
                        '        <image:loc>http://foo/image.jpg</image:loc>'
                        '    </image:image>'
                        '</url>'
                        '</urlset>')
        self.file.close()
        sitemap_document = SitemapXmlDocument(self.file.name, self.url)
        self.assertEqual(SiteMapType.SITEMAP_XML,
                         sitemap_document.get_sitemap_type())
        self.assertEqual(["http://foo/bar"], list(sitemap_document.get_urls()))
        self.assertEqual(1, sitemap_document.valid_urls)
        self.assertEqual(0, sitemap_document.invalid_urls)

    def test_xml_parsing_error(self):
        self.file.write('<urlset><url></url>')
        self.file.close()
        sitemap_document = SitemapXmlDocument(self.file.name, self.url)
        self.assertEqual(SiteMapType.SITEMAP_XML, sitemap_document.get_sitemap_type())
        self.assertRaises(
            ParsingError,
            list,
            sitemap_document.get_urls())
        self.assertEqual(0, sitemap_document.valid_urls)
        self.assertEqual(0, sitemap_document.invalid_urls)

    def test_not_sitemap(self):
        self.file.write('<foo></foo>')  # valid xml but not a sitemap
        self.file.close()
        sitemap_document = SitemapXmlDocument(self.file.name, self.url)
        self.assertEqual(SiteMapType.SITEMAP_XML, sitemap_document.get_sitemap_type())
        self.assertEqual([], list(sitemap_document.get_urls()))
        self.assertEqual(0, sitemap_document.valid_urls)
        self.assertEqual(0, sitemap_document.invalid_urls)


class TestSitemapIndexXmlDocument(unittest.TestCase):
    def setUp(self):
        self.file = tempfile.NamedTemporaryFile(delete=False)
        self.url = "http://foo/sitemap_index.xml"

    def tearDown(self):
        os.remove(self.file.name)

    def test_nominal_case(self):
        self.file.write('<?xml version="1.0" encoding="UTF-8"?>'
                        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                        '<sitemap><loc>http://foo/sitemap.xml.gz</loc></sitemap>'
                        '</sitemapindex>')
        self.file.close()
        sitemap_document = SitemapIndexXmlDocument(self.file.name, self.url)

        self.assertEqual(SiteMapType.SITEMAP_INDEX,
                         sitemap_document.get_sitemap_type())
        self.assertEqual(["http://foo/sitemap.xml.gz"],
                         list(sitemap_document.get_urls()))
        self.assertEqual(1, sitemap_document.valid_urls)
        self.assertEqual(0, sitemap_document.invalid_urls)

    def test_no_namespace(self):
        self.file.write('<sitemapindex>'
                        '<sitemap><loc>http://foo/bar</loc></sitemap>'
                        '</sitemapindex>')
        self.file.close()
        sitemap_document = SitemapIndexXmlDocument(self.file.name, self.url)
        self.assertEqual(SiteMapType.SITEMAP_INDEX,
                         sitemap_document.get_sitemap_type())
        self.assertEqual(["http://foo/bar"],
                         list(sitemap_document.get_urls()))
        self.assertEqual(1, sitemap_document.valid_urls)
        self.assertEqual(0, sitemap_document.invalid_urls)

    def test_invalid_url(self):
        self.file.write('<?xml version="1.0" encoding="UTF-8"?>'
                        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                        '<sitemap>'
                        '<loc>http://foo/sitemap.1.xml.gz</loc>'
                        '<loc>foo</loc>'
                        '<loc>http://foo/sitemap.2.xml.gz</loc>'
                        '</sitemap>'
                        '</sitemapindex>')
        self.file.close()
        sitemap_document = SitemapIndexXmlDocument(self.file.name, self.url)
        self.assertEqual(["http://foo/sitemap.1.xml.gz", "http://foo/sitemap.2.xml.gz"],
                         list(sitemap_document.get_urls()))
        self.assertEqual(2, sitemap_document.valid_urls)
        self.assertEqual(1, sitemap_document.invalid_urls)

    def test_forbidden_url(self):
        self.file.write('<?xml version="1.0" encoding="UTF-8"?>'
                        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                        '<sitemap>'
                        '<loc>http://foo/sitemap.1.xml.gz</loc>'
                        '<loc>http://bar/sitemap.xml</loc>'  # this url is on a different domain
                        '<loc>http://foo/sitemap.2.xml.gz</loc>'
                        '</sitemap>'
                        '</sitemapindex>')
        self.file.close()
        sitemap_document = SitemapIndexXmlDocument(self.file.name, self.url)
        self.assertEqual(["http://foo/sitemap.1.xml.gz", "http://foo/sitemap.2.xml.gz"],
                         list(sitemap_document.get_urls()))
        self.assertEqual(2, sitemap_document.valid_urls)
        self.assertEqual(1, sitemap_document.invalid_urls)

class TestSitemapRssDocument(unittest.TestCase):
    def setUp(self):
        self.file = tempfile.NamedTemporaryFile(delete=False)
        self.url = "http://foo/sitemap.xml"

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
        sitemap_document = SitemapRssDocument(self.file.name, self.url)
        self.assertEqual(SiteMapType.SITEMAP_RSS,
                         sitemap_document.get_sitemap_type())
        expected_urls = [
            "http://www.example.com/main.html",
            "http://www.example.com/blog/post/1"
        ]

        self.assertEqual(expected_urls, list(sitemap_document.get_urls()))
        self.assertEqual(2, sitemap_document.valid_urls)
        self.assertEqual(0, sitemap_document.invalid_urls)

    def test_invalid_xml(self):
        self.file.write('<foo><bar>')
        self.file.close()
        sitemap_document = SitemapRssDocument(self.file.name, self.url)
        self.assertRaises(ParsingError,
                          list,
                          sitemap_document.get_urls())
        self.assertEqual(0, sitemap_document.valid_urls)
        self.assertEqual(0, sitemap_document.invalid_urls)

    def test_invalid_url(self):
        self.file.write('<?xml version="1.0" encoding="UTF-8" ?>'
                         '<rss version="2.0">'
                         '<channel>'
                         ' <title>RSS Title</title>'
                         ' <description>This is an example of an RSS feed</description>'
                         ' <link>http://www.example.com/main.html</link>'
                         ' <title>Invalid url</title>'
                         ' <link>foo</link>'
                         ' <item>'
                         '  <title>Example entry</title>'
                         '  <description>Here is some text containing an interesting description.</description>'
                         '  <link>http://www.example.com/blog/post/1</link>'
                         ' </item>'
                         '</channel>'
                         '</rss>')
        self.file.close()
        sitemap_document = SitemapRssDocument(self.file.name, self.url)
        expected_urls = [
            "http://www.example.com/main.html",
            "http://www.example.com/blog/post/1"
        ]

        self.assertEqual(expected_urls, list(sitemap_document.get_urls()))
        self.assertEqual(2, sitemap_document.valid_urls)
        self.assertEqual(1, sitemap_document.invalid_urls)


class TestSitemapTextDocument(unittest.TestCase):
    def setUp(self):
        self.file = tempfile.NamedTemporaryFile(delete=False)
        self.url = "http://foo/sitemap.txt"

    def tearDown(self):
        os.remove(self.file.name)

    def test_nominal_case(self):
        self.file.write('http://foo.com/bar\n'
                        'http://foo.com/baz\n'
                        'http://foo.com/qux\n')
        self.file.close()
        sitemap_document = SitemapTextDocument(self.file.name, self.url)
        self.assertEqual(SiteMapType.SITEMAP_TEXT,
                         sitemap_document.get_sitemap_type())
        expected_urls = [
            "http://foo.com/bar",
            "http://foo.com/baz",
            "http://foo.com/qux"
        ]

        self.assertEqual(expected_urls, list(sitemap_document.get_urls()))
        self.assertEqual(3, sitemap_document.valid_urls)
        self.assertEqual(0, sitemap_document.invalid_urls)

    def test_long_line(self):
        self.file.write('http://foo.com/bar\n')
        #long line
        self.file.write('{}\n'.format('-' * 8192))
        self.file.write('http://foo.com/baz\n')
        self.file.close()
        sitemap_document = SitemapTextDocument(self.file.name, self.url)
        self.assertEqual(SiteMapType.SITEMAP_TEXT,
                         sitemap_document.get_sitemap_type())
        expected_urls = [
            "http://foo.com/bar",
            "http://foo.com/baz"
        ]

        self.assertEqual(expected_urls, list(sitemap_document.get_urls()))
        self.assertEqual(2, sitemap_document.valid_urls)
        self.assertEqual(1, sitemap_document.invalid_urls)

    def test_very_long_line(self):
        self.file.write('http://foo.com/bar\n')
        #very long line
        line_length = 2 * csv.field_size_limit()
        self.file.write('{}\n'.format('-' * line_length))
        self.file.write('http://foo.com/baz\n')
        self.file.close()
        sitemap_document = SitemapTextDocument(self.file.name, self.url)
        self.assertEqual(SiteMapType.SITEMAP_TEXT,
                         sitemap_document.get_sitemap_type())
        expected_urls = [
            "http://foo.com/bar",
            "http://foo.com/baz"
        ]

        self.assertEqual(expected_urls, list(sitemap_document.get_urls()))
        self.assertEqual(2, sitemap_document.valid_urls)
        self.assertEqual(1, sitemap_document.invalid_urls)


    def test_invalid_urls(self):
        self.file.write('http://foo.com/bar\n'
                        'foo.com/baz\n'
                        'http://foo.com/qux\n')
        self.file.close()
        sitemap_document = SitemapTextDocument(self.file.name, self.url)
        self.assertEqual(SiteMapType.SITEMAP_TEXT,
                         sitemap_document.get_sitemap_type())
        expected_urls = [
            "http://foo.com/bar",
            "http://foo.com/qux"
        ]

        self.assertEqual(expected_urls, list(sitemap_document.get_urls()))
        self.assertEqual(2, sitemap_document.valid_urls)
        self.assertEqual(1, sitemap_document.invalid_urls)

    def test_empty_line(self):
        self.file.write('http://foo.com/bar\n'
                        '\n'
                        'http://foo.com/qux\n')
        self.file.close()
        sitemap_document = SitemapTextDocument(self.file.name, self.url)
        self.assertEqual(SiteMapType.SITEMAP_TEXT,
                         sitemap_document.get_sitemap_type())
        expected_urls = [
            "http://foo.com/bar",
            "http://foo.com/qux"
        ]

        self.assertEqual(expected_urls, list(sitemap_document.get_urls()))
        self.assertEqual(2, sitemap_document.valid_urls)
        #empty lines are considered as invalid lines
        self.assertEqual(1, sitemap_document.invalid_urls)


class TestUrlValidator(unittest.TestCase):
    def test_nominal_case(self):
        self.assertTrue(UrlValidator.is_valid("http://foo.com"))
        self.assertTrue(UrlValidator.is_valid("https://foo.com"))

    def test_case_insensitivity(self):
        self.assertTrue(UrlValidator.is_valid("HTTP://foo.com"))
        self.assertTrue(UrlValidator.is_valid("HtTpS://foo.com"))

    def test_only_protocol(self):
        self.assertTrue(UrlValidator.is_valid("http://"))

    def test_no_protocol(self):
        self.assertFalse(UrlValidator.is_valid("://foo.com"))

    def test_invalid_protocol(self):
        self.assertFalse(UrlValidator.is_valid("ftp://foo.com"))

    def test_long_lines(self):
        url = "http://{}".format('a' * UrlValidator.MAXIMUM_LENGTH)
        self.assertTrue(UrlValidator.is_valid(url[:UrlValidator.MAXIMUM_LENGTH]))
        self.assertFalse(UrlValidator.is_valid(url[:UrlValidator.MAXIMUM_LENGTH + 1]))


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

    def setUp(self):
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        self.tmp_file_path = tmp_file.name
        self.url = "http://foo.com/bar"

    def tearDown(self):
        os.remove(self.tmp_file_path)

    def test_xml_sitemap(self):
        with open(self.tmp_file_path, "w") as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>'
                    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                    '<url><loc>http://foo/bar</loc></url>'
                    '</urlset>')
        self.assertEqual(SiteMapType.SITEMAP_XML,
                         guess_sitemap_type(self.tmp_file_path, self.url))

    def test_xml_sitemapindex(self):
        with open(self.tmp_file_path, "w") as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>'
                    '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                    '<sitemap><loc>http://foo/sitemap.xml.gz</loc></sitemap>'
                    '</sitemapindex>')
        self.assertEqual(SiteMapType.SITEMAP_INDEX,
                         guess_sitemap_type(self.tmp_file_path, self.url))

    def test_rss_sitemap(self):
        with open(self.tmp_file_path, "w") as f:
            f.write('<?xml version="1.0" encoding="UTF-8" ?>'
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
                         guess_sitemap_type(self.tmp_file_path, self.url))

    def test_xml_syntax_error(self):
        with open(self.tmp_file_path, "w") as f:
            f.write('<foo></bar>')
        self.assertEqual(SiteMapType.UNKNOWN,
                         guess_sitemap_type(self.tmp_file_path, self.url))

    def test_simple_xml(self):
        with open(self.tmp_file_path, "w") as f:
            f.write('<foo></foo>')
        self.assertEqual(SiteMapType.UNKNOWN,
                         guess_sitemap_type(self.tmp_file_path, self.url))

    def test_simple_text_file(self):
        with open(self.tmp_file_path, "w") as f:
            f.write('http://foo\nhttps://bar')
        self.assertEqual(SiteMapType.SITEMAP_TEXT,
                         guess_sitemap_type(self.tmp_file_path, self.url))

    def test_text_file_one_url(self):
        with open(self.tmp_file_path, "w") as f:
            f.write('foo\r\nbar\r\nhttp://baz')
        self.assertEqual(SiteMapType.SITEMAP_TEXT,
                         guess_sitemap_type(self.tmp_file_path, self.url))

    def test_text_file_no_urls(self):
        with open(self.tmp_file_path, "w") as f:
            f.write('foo\nbar')
        self.assertEqual(SiteMapType.UNKNOWN,
                         guess_sitemap_type(self.tmp_file_path, self.url))

@mock.patch("cdf.features.sitemaps.document.guess_sitemap_type", autospec=True)
class TestInstanciateSitemapDocument(unittest.TestCase):
    def setUp(self):
        self.file_path = "/tmp/foo"
        self.url = "http://foo.xml"

    def test_xml_sitemap(self, guess_sitemap_type_mock):
        guess_sitemap_type_mock.return_value = SiteMapType.SITEMAP_XML
        actual_result = instanciate_sitemap_document(self.file_path, self.url)
        self.assertIsInstance(actual_result, SitemapXmlDocument)

    def test_sitemap_index(self, guess_sitemap_type_mock):
        guess_sitemap_type_mock.return_value = SiteMapType.SITEMAP_INDEX
        actual_result = instanciate_sitemap_document(self.file_path, self.url)
        self.assertIsInstance(actual_result, SitemapIndexXmlDocument)
        self.assertEqual(self.url, actual_result.url)

    def test_rss_sitemap(self, guess_sitemap_type_mock):
        guess_sitemap_type_mock.return_value = SiteMapType.SITEMAP_RSS
        actual_result = instanciate_sitemap_document(self.file_path, self.url)
        self.assertIsInstance(actual_result, SitemapRssDocument)

    def test_text_sitemap(self, guess_sitemap_type_mock):
        guess_sitemap_type_mock.return_value = SiteMapType.SITEMAP_TEXT
        actual_result = instanciate_sitemap_document(self.file_path, self.url)
        self.assertIsInstance(actual_result, SitemapTextDocument)

    def test_unknown_format(self, guess_sitemap_type_mock):
        guess_sitemap_type_mock.return_value = SiteMapType.UNKNOWN
        self.assertRaises(UnhandledFileType,
                          instanciate_sitemap_document,
                          "foo",
                          self.url)

class TestSitemapUrlValidator(unittest.TestCase):
    def setUp(self):
        self.validator = SitemapUrlValidator("http://foo.com/bar/sitemap_index.xml")

    def test_same_domain(self):
        self.assertTrue(self.validator.is_valid("http://foo.com/bar/sitemap.xml"))
        #the sitemap is in a subdirectory
        self.assertTrue(self.validator.is_valid("http://foo.com/bar/baz/sitemap.xml"))
        #the sitemap index is in a subdirectory (not supported by the standard)
        self.assertTrue(self.validator.is_valid("http://foo.com/sitemap.xml"))
        #the protocols are different
        self.assertTrue(self.validator.is_valid("https://foo.com/bar/sitemap.xml"))

    def test_sitemap_different_domains(self):
        self.assertFalse(self.validator.is_valid("http://bar.com/sitemap.xml"))

    def test_sitemap_in_subdomain(self):
        self.assertTrue(self.validator.is_valid("http://foo.foo.com/sitemap.xml"))

    def test_sitemap_index_in_subdomain(self):
        sitemap_url_validator = SitemapUrlValidator("http://foo.foo.com/sitemap_index.xml")
        self.assertFalse(sitemap_url_validator.is_valid("http://foo.com/sitemap.xml"))

    def test_sitemap_index_in_www(self):
        validator = SitemapUrlValidator("http://www.foo.com/sitemap_index.xml")
        #if the sitemap index is on the "www", it can reference all the subdomains
        self.assertTrue(validator.is_valid("http://foo.foo.com/sitemap.xml"))
        self.assertTrue(validator.is_valid("http://foo.www.foo.com/sitemap.xml"))

        self.assertFalse(validator.is_valid("http://bar.com/sitemap.xml"))


