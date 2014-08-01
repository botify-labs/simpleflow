import unittest
import json
from cdf.features.sitemap.metadata import (Metadata,
                                           SitemapMetadata,
                                           SitemapIndexMetadata,
                                           Error,
                                           parse_sitemap_metadata,
                                           parse_sitemap_index_metadata)
from cdf.features.sitemap.document import SiteMapType


class TestSitemapMetadata(unittest.TestCase):
    def setUp(self):
        self.url = "http://foo.com/sitemap.xml"
        self.s3_uri = "s3://foo.com/sitemap.xml"
        self.sitemap_index = "http://foo.com/sitemap_index.xml"

        self.error_type = "ParsingError"
        self.error_message = "error message"

        self.valid_urls = 10
        self.invalid_urls = 5

    def test_to_dict_nominal_case(self):
        sitemap_metadata = SitemapMetadata(self.url,
                                           self.s3_uri,
                                           [self.sitemap_index])

        expected_result = {
            "url": self.url,
            "s3_uri": self.s3_uri,
            "sitemap_indexes": [self.sitemap_index]
        }

        self.assertEqual(expected_result, sitemap_metadata.to_dict())

    def test_to_dict_no_sitemap_index(self):
        sitemap_metadata = SitemapMetadata(self.url,
                                           self.s3_uri)

        expected_result = {
            "url": self.url,
            "s3_uri": self.s3_uri,
        }

        self.assertEqual(expected_result, sitemap_metadata.to_dict())

    def test_to_dict_error_case(self):
        sitemap_metadata = SitemapMetadata(self.url,
                                           self.s3_uri)
        sitemap_metadata.error_type = self.error_type
        sitemap_metadata.error_message = self.error_message
        expected_result = {
            "url": self.url,
            "s3_uri": self.s3_uri,
            "error": self.error_type,
            "message": self.error_message
        }

        self.assertEqual(expected_result, sitemap_metadata.to_dict())

    def test_to_dict_valid_invalid_urls(self):
        sitemap_metadata = SitemapMetadata(self.url,
                                           self.s3_uri)
        sitemap_metadata.valid_urls = self.valid_urls
        sitemap_metadata.invalid_urls = self.invalid_urls

        expected_result = {
            "url": self.url,
            "s3_uri": self.s3_uri,
            "valid_urls": self.valid_urls,
            "invalid_urls": self.invalid_urls
        }

        self.assertEqual(expected_result, sitemap_metadata.to_dict())


class TestSitemapIndexMetadata(unittest.TestCase):
    def setUp(self):
        self.url = "http://foo.com/sitemap_index.xml"
        self.valid_urls = 10
        self.invalid_urls = 5
        self.error_type = "ParsingError"
        self.error_message = "error message"

    def test_to_dict_nominal_case(self):
        sitemap_index_metadata = SitemapIndexMetadata(self.url,
                                                      self.valid_urls,
                                                      self.invalid_urls)
        expected_result = {
            "url": self.url,
            "valid_urls": self.valid_urls,
            "invalid_urls": self.invalid_urls
        }
        self.assertEqual(expected_result, sitemap_index_metadata.to_dict())

    def test_to_dict_error_case(self):
        sitemap_index_metadata = SitemapIndexMetadata(self.url,
                                                      self.valid_urls,
                                                      self.invalid_urls,
                                                      self.error_type,
                                                      self.error_message)
        expected_result = {
            "url": self.url,
            "valid_urls": self.valid_urls,
            "invalid_urls": self.invalid_urls,
            "error": self.error_type,
            "message": self.error_message
        }
        self.assertEqual(expected_result, sitemap_index_metadata.to_dict())


class TestMetadata(unittest.TestCase):
    def setUp(self):
        self.sitemap = "http://foo/sitemap.xml"
        self.sitemap_2 = "http://foo/sitemap_2.xml"

        self.sitemap_index = "http://foo/sitemap_index.xml"
        self.sitemap_index_2 = "http://foo/sitemap_index_2.xml"

    def test_add_success_sitemap(self):
        metadata = Metadata()
        metadata.add_success_sitemap(SitemapMetadata(self.sitemap, "/tmp/sitemap.xml"))
        metadata.add_success_sitemap(SitemapMetadata(self.sitemap_2, "/tmp/sitemap_2.xml"))

        expected_result = [
            SitemapMetadata(self.sitemap, "/tmp/sitemap.xml"),
            SitemapMetadata(self.sitemap_2, "/tmp/sitemap_2.xml")
        ]
        self.assertEqual(expected_result, metadata.sitemaps)

        #readd a sitemap index
        metadata.add_success_sitemap(SitemapMetadata(self.sitemap, "/tmp/sitemap.xml_2"))
        self.assertEqual(expected_result, metadata.sitemaps)

    def test_add_success_sitemap_index(self):
        metadata = Metadata()
        metadata.add_success_sitemap_index(SitemapIndexMetadata(self.sitemap_index, 0, 0))
        metadata.add_success_sitemap_index(SitemapIndexMetadata(self.sitemap_index_2, 0, 0))

        expected_result = [
            SitemapIndexMetadata(self.sitemap_index, 0, 0),
            SitemapIndexMetadata(self.sitemap_index_2, 0, 0)
        ]
        self.assertEqual(expected_result, metadata.sitemap_indexes)

        #readd a sitemap index
        metadata.add_success_sitemap_index(SitemapIndexMetadata(self.sitemap_index, 10, 0))
        self.assertEqual(expected_result, metadata.sitemap_indexes)

    def test_add_error(self):
        metadata = Metadata()
        metadata.add_error("http://foo.com/bar.xml", SiteMapType.SITEMAP_XML, "ParsingError", "Error message")
        metadata.add_error("http://foo.com/baz.xml", SiteMapType.UNKNOWN, "DownloadError", "Error message")

        expected_result = [
            Error("http://foo.com/bar.xml", SiteMapType.SITEMAP_XML, "ParsingError", "Error message"),
            Error("http://foo.com/baz.xml", SiteMapType.UNKNOWN, "DownloadError", "Error message")
        ]
        self.assertEqual(expected_result, metadata.errors)

        #readd a sitemap index
        metadata.add_error("http://foo.com/bar.xml", SiteMapType.SITEMAP_XML, "DownloadError", "Error message")
        self.assertEqual(expected_result, metadata.errors)


    def test_to_json(self):
        download_status = Metadata(
            [SitemapMetadata("http://foo/sitemap_1.xml",
                             "s3://foo/sitemap_1.xml",
                             [self.sitemap_index])],
            [SitemapIndexMetadata("http://foo/sitemap_index.xml", 2, 1)],
            [Error("http://error1", SiteMapType.UNKNOWN, "foo", "bar"),
             Error("http://error2", SiteMapType.UNKNOWN, "foo", "bar")]
        )

        actual_result = download_status.to_json()

        expected_result = {
            "sitemaps": [
                {
                    "url": "http://foo/sitemap_1.xml",
                    "s3_uri": "s3://foo/sitemap_1.xml",
                    "sitemap_indexes": ["http://foo/sitemap_index.xml"]
                }
            ],
            "sitemap_indexes": [
                {
                    "url": "http://foo/sitemap_index.xml",
                    "valid_urls": 2,
                    "invalid_urls": 1
                }
            ],
            "errors": [
                {
                    "url": "http://error1",
                    "file_type": "UNKNOWN",
                    "error": "foo",
                    "message": "bar"
                },
                {
                    "url": "http://error2",
                    "file_type": "UNKNOWN",
                    "error": "foo",
                    "message": "bar"
                }
            ]
        }
        #compare the objects instead of the json representation
        #to be insensitive to item ordering
        self.assertEqual(expected_result["sitemap_indexes"], json.loads(actual_result)["sitemap_indexes"])
        self.assertEqual(expected_result, json.loads(actual_result))

    def test_to_json_no_sitemap(self):
        download_status = Metadata(
            [SitemapMetadata("http://foo/sitemap_1.xml",
                             "s3://foo/sitemap_1.xml",
                             None)]
        )

        actual_result = download_status.to_json()

        expected_result = {
            "sitemaps": [
                {
                    "url": u"http://foo/sitemap_1.xml",
                    "s3_uri": u"s3://foo/sitemap_1.xml",
                },
            ],
            "sitemap_indexes": [],
            "errors": []
        }
        #compare the objects instead of the json representation
        #to be insensitive to item ordering
        self.assertEqual(expected_result, json.loads(actual_result))


class TestSitemapMetadataHasBeenProcessed(unittest.TestCase):
    def setUp(self):
        self.url1 = "http://foo.com/bar"
        self.url2 = "http://foo.com/qux"
        self.metadata = Metadata()

    def test_empty_object(self):
        self.assertFalse(self.metadata.is_success_sitemap(self.url1))

    def test_sitemap(self):
        self.metadata.add_success_sitemap(SitemapMetadata(self.url1, None))
        self.assertTrue(self.metadata.is_success_sitemap(self.url1))
        self.assertFalse(self.metadata.is_success_sitemap(self.url2))

    def test_sitemap_index(self):
        self.metadata.add_success_sitemap_index(
            SitemapIndexMetadata(self.url1, 0, 0)
        )
        self.assertFalse(self.metadata.is_success_sitemap(self.url1))

    def test_error(self):
        self.metadata.add_error(self.url1, SiteMapType.UNKNOWN, "Error", "")
        #we do not check errors
        self.assertFalse(self.metadata.is_success_sitemap(self.url1))


class TestParseSitemapMetadata(unittest.TestCase):
    def setUp(self):
        self.url = "http://foo.com/sitemap.xml"
        self.s3_uri = "s3://foo.com/sitemap.xml"
        self.sitemap_index = "http://foo.com/sitemap_index.xml"
        self.valid_urls = 10
        self.invalid_urls = 5
        self.error_type = "ParsingError"
        self.error_message = "error message"

    def test_nominal_case(self):
        input_dict = {
            "url": self.url,
            "s3_uri": self.s3_uri
        }
        expected_result = SitemapMetadata(self.url, self.s3_uri)
        self.assertEqual(expected_result, parse_sitemap_metadata(input_dict))

    def test_to_dict_valid_invalid_urls(self):
        input_dict = {
            "url": self.url,
            "s3_uri": self.s3_uri,
            "valid_urls": self.valid_urls,
            "invalid_urls": self.invalid_urls
        }

        expected_result = SitemapMetadata(self.url, self.s3_uri)
        expected_result.valid_urls = self.valid_urls
        expected_result.invalid_urls = self.invalid_urls
        self.assertEqual(expected_result, parse_sitemap_metadata(input_dict))

    def test_to_dict_sitemap_index(self):
        input_dict = {
            "url": self.url,
            "s3_uri": self.s3_uri,
            "sitemap_index": self.sitemap_index
        }
        expected_result = SitemapMetadata(self.url, self.s3_uri)
        expected_result.sitemap_index = self.sitemap_index
        self.assertEqual(expected_result, parse_sitemap_metadata(input_dict))

    def test_to_dict_error_case(self):
        input_dict = {
            "url": self.url,
            "s3_uri": self.s3_uri,
            "error": self.error_type,
            "message": self.error_message
        }
        expected_result = SitemapMetadata(self.url, self.s3_uri)
        expected_result.error_type = self.error_type
        expected_result.error_message = self.error_message
        self.assertEqual(expected_result, parse_sitemap_metadata(input_dict))

    #TODO add test with unknown fields


class TestParseSitemapIndexMetadata(unittest.TestCase):
    def setUp(self):
        self.url = "http://foo.com/sitemap_index.xml"
        self.valid_urls = 10
        self.invalid_urls = 5
        self.error_type = "ParsingError"
        self.error_message = "error message"

    def test_nominal_case(self):
        input_dict = {
            "url": self.url,
            "valid_urls": self.valid_urls,
            "invalid_urls": self.invalid_urls
        }
        expected_result = SitemapIndexMetadata(self.url,
                                               self.valid_urls,
                                               self.invalid_urls)
        self.assertEqual(expected_result,
                         parse_sitemap_index_metadata(input_dict))

    def test_error_case(self):
        input_dict = {
            "url": self.url,
            "valid_urls": self.valid_urls,
            "invalid_urls": self.invalid_urls,
            "error": self.error_type,
            "message": self.error_message
        }
        expected_result = SitemapIndexMetadata(self.url,
                                               self.valid_urls,
                                               self.invalid_urls)
        expected_result.error_type = self.error_type
        expected_result.error_message = self.error_message
        self.assertEqual(expected_result,
                         parse_sitemap_index_metadata(input_dict))

