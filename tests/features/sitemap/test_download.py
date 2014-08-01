import unittest
import mock
import os
from cdf.features.sitemap.metadata import (Metadata,
                                           Error,
                                           SitemapMetadata,
                                           SitemapIndexMetadata)

from cdf.features.sitemap.download import (download_sitemaps,
                                           download_sitemaps_from_sitemap_index,
                                           get_output_file_path)
from cdf.features.sitemap.document import (SiteMapType,
                                           SitemapXmlDocument,
                                           SitemapIndexXmlDocument)
from cdf.features.sitemap.exceptions import (DownloadError,
                                             ParsingError,
                                             UnhandledFileType)

class TestGetOutputFilePath(unittest.TestCase):
    def setUp(self):
        self.input_url = "http://bar/sitemap.xml"
        self.output_dir = "/tmp/foo"

    def test_no_collision_case(self):
        self.assertEqual("/tmp/foo/sitemap.xml",
                         get_output_file_path(self.input_url, self.output_dir))

    @mock.patch("os.path.exists")
    def test_collision_case(self, exist_mock):
        def side_effect(arg):
            return arg in ["/tmp/foo/sitemap.xml", "/tmp/foo/sitemap.xml_2"]
        exist_mock.side_effect = side_effect
        self.assertEqual("/tmp/foo/sitemap.xml_3",
                         get_output_file_path(self.input_url, self.output_dir))


class TestDownloadSiteMaps(unittest.TestCase):
    def setUp(self):
        self.sitemap_url = "http://bar/sitemap.xml"
        self.sitemap_index_url = "http://bar/sitemap_index.xml"
        self.output_dir = "/tmp/foo"
        self.user_agent = "custom user-agent"

        self.sitemap_mock = mock.create_autospec(SitemapXmlDocument)
        self.sitemap_mock.get_sitemap_type.return_value = SiteMapType.SITEMAP_XML

        self.sitemap_index_mock = SitemapIndexXmlDocument("/tmp/foo", self.sitemap_index_url)

    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    def test_sitemap_case(self,
                          instanciate_sitemap_document_mock,
                          download_url_mock):
        instanciate_sitemap_document_mock.return_value = self.sitemap_mock

        metadata = Metadata()
        download_sitemaps(self.sitemap_url,
                          self.output_dir,
                          self.user_agent,
                          metadata)
        expected_metadata = Metadata()
        expected_metadata.add_success_sitemap(
            SitemapMetadata(self.sitemap_url, "/tmp/foo/sitemap.xml")
        )
        self.assertEqual(expected_metadata, metadata)
        download_url_mock.assert_called_once_with(self.sitemap_url,
                                                  "/tmp/foo/sitemap.xml",
                                                  self.user_agent)


    @mock.patch("os.remove", autospec=True)
    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    @mock.patch("cdf.features.sitemap.download.download_sitemaps_from_sitemap_index", autospec=True)
    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    def test_sitemap_index_case(self,
                                instanciate_sitemap_document_mock,
                                download_sitemaps_from_sitemap_index_mock,
                                download_url_mock,
                                remove_mock):
        self.sitemap_index_mock.get_urls = mock.MagicMock()
        self.sitemap_index_mock.get_urls.return_value = iter(self.sitemap_url)
        instanciate_sitemap_document_mock.return_value = self.sitemap_index_mock

        download_status = Metadata()
        download_status.add_success_sitemap(
            SitemapMetadata("http://foo", "s3://foo", self.sitemap_url)
        )
        download_status.add_success_sitemap_index(SitemapIndexMetadata(self.sitemap_index_url, 0, 0))
        input_url = self.sitemap_index_url
        metadata = Metadata()
        download_sitemaps(input_url,
                          self.output_dir,
                          self.user_agent,
                          metadata)
        expected_result = Metadata()
        expected_result.add_success_sitemap(
            SitemapMetadata("http://foo", "s3://foo", self.sitemap_url)
        )
        expected_result.add_success_sitemap_index(SitemapIndexMetadata(self.sitemap_index_url, 0, 0))

        download_url_mock.assert_called_once_with(self.sitemap_index_url,
                                                  "/tmp/foo/sitemap_index.xml",
                                                  self.user_agent)
        self.assertEqual(1, download_sitemaps_from_sitemap_index_mock.call_count)
        remove_mock.assert_called_once_with("/tmp/foo/sitemap_index.xml")

    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    def test_not_sitemap_file(self,
                              instanciate_sitemap_document_mock,
                              download_url_mock):
        instanciate_sitemap_document_mock.side_effect = UnhandledFileType("foo")
        input_url = "http://foo/bar.xml"
        metadata = Metadata()
        download_sitemaps(input_url, self.output_dir, self.user_agent, metadata)

        expected_metadata = Metadata()
        expected_metadata.add_error(
            Error(input_url, SiteMapType.UNKNOWN, "UnhandledFileType", "foo")
        )
        self.assertEqual(expected_metadata, metadata)

        download_url_mock.assert_called_once_with("http://foo/bar.xml",
                                                  "/tmp/foo/bar.xml",
                                                  self.user_agent)

    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    def test_download_error(self,
                            download_url_mock):
        download_url_mock.side_effect = DownloadError("foo")

        metadata = Metadata()
        download_sitemaps(self.sitemap_url,
                          self.output_dir,
                          self.user_agent,
                          metadata)
        expected_metadata = Metadata()
        expected_metadata.add_error(
            Error(self.sitemap_url, SiteMapType.UNKNOWN, "DownloadError", "foo")
        )
        self.assertEqual(expected_metadata, metadata)


    @mock.patch("os.remove", autospec=True)
    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    def test_parsing_error(self,
                           instanciate_sitemap_document_mock,
                           download_url_mock,
                           remove_mock):
        def url_generator():
            raise ParsingError("error message")
            yield "http://foo.com"
        self.sitemap_index_mock.get_urls = url_generator
        instanciate_sitemap_document_mock.return_value = self.sitemap_index_mock

        metadata = Metadata()
        download_sitemaps(self.sitemap_index_url,
                          self.output_dir,
                          self.user_agent,
                          metadata)
        expected_metadata = Metadata()
        expected_metadata.add_error(
            Error(self.sitemap_index_url, SiteMapType.SITEMAP_INDEX, "ParsingError", "error message")
        )
        self.assertEqual(expected_metadata, metadata)
        remove_mock.assert_called_once_with("/tmp/foo/sitemap_index.xml")
        download_url_mock.assert_called_once_with(self.sitemap_index_url,
                                                  os.path.join(self.output_dir, "sitemap_index.xml"),
                                                  self.user_agent)

    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    def test_already_processed_url(self, download_url_mock):
        metadata = Metadata()
        metadata.add_success_sitemap(
            SitemapMetadata(self.sitemap_url, "s3://foo.com/sitemap.xml")
        )
        download_sitemaps(self.sitemap_url,
                          self.output_dir,
                          self.user_agent,
                          metadata)
        #check that we returned directly
        self.assertFalse(download_url_mock.called)


class TestDownloadSitemapsFromSitemapIndex(unittest.TestCase):
    def setUp(self):
        self.urls = [
            "http://foo/bar.xml",
            "http://foo/baz.xml"
        ]

        self.output_dir = "/tmp/foo"
        self.user_agent = 'custom user-agent'
        self.expected_download_calls = [
            mock.call("http://foo/bar.xml", "/tmp/foo/bar.xml", self.user_agent),
            mock.call("http://foo/baz.xml", "/tmp/foo/baz.xml", self.user_agent)
        ]

        self.user_agent = "custom user-agent"

        self.sitemap_mock = SitemapXmlDocument("/tmp/bar.xml", "http://foo/bar.xml")

        self.unknown_sitemap_mock = mock.create_autospec(SitemapXmlDocument)
        self.unknown_sitemap_mock.get_sitemap_type.return_value = SiteMapType.UNKNOWN

        self.sitemap_index_mock = SitemapIndexXmlDocument("/tmp/sitemap_index.xml",
                                                          "http://foo/sitemap_index.xml")
        self.sitemap_index_mock.get_urls = mock.MagicMock()
        self.sitemap_index_mock.get_urls.return_value = iter(self.urls)

    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    def test_nominal_case(self,
                          instanciate_sitemap_document_mock,
                          download_url_mock):
        instanciate_sitemap_document_mock.return_value = self.sitemap_mock

        actual_result = Metadata()
        download_sitemaps_from_sitemap_index(self.sitemap_index_mock,
                                             self.output_dir,
                                             self.user_agent,
                                             actual_result)
        expected_result = Metadata()
        expected_result.add_success_sitemap(
            SitemapMetadata("http://foo/bar.xml",
                            "/tmp/foo/bar.xml",
                            [self.sitemap_index_mock.url])
        )
        expected_result.add_success_sitemap(
            SitemapMetadata("http://foo/baz.xml",
                            "/tmp/foo/baz.xml",
                            [self.sitemap_index_mock.url])
        )
        expected_result.add_success_sitemap_index(SitemapIndexMetadata(self.sitemap_index_mock.url, 0, 0))
        self.assertEqual(expected_result, actual_result)
        self.assertEqual(self.expected_download_calls,
                         download_url_mock.mock_calls)

    def test_empty_list(self):
        document = SitemapIndexXmlDocument("/tmp/foo", "http://foo")
        document.get_urls = mock.MagicMock()
        document.get_urls.return_value = iter([])
        actual_result = Metadata()
        download_sitemaps_from_sitemap_index(document,
                                             self.output_dir,
                                             self.user_agent,
                                             actual_result)
        expected_result = Metadata()
        expected_result.add_success_sitemap_index(SitemapIndexMetadata("http://foo", 0, 0))
        self.assertEqual(expected_result, actual_result)

    @mock.patch("os.remove", autospec=True)
    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    def test_sitemap_index_file(self,
                                instanciate_sitemap_document_mock,
                                download_url_mock,
                                remove_mock):
        instanciate_sitemap_document_mock.side_effect = iter([
            self.sitemap_index_mock,
            self.sitemap_mock
        ])

        actual_result = Metadata()
        download_sitemaps_from_sitemap_index(self.sitemap_index_mock,
                                             self.output_dir,
                                             self.user_agent,
                                             actual_result)

        expected_result = Metadata()
        expected_result.add_success_sitemap(
            SitemapMetadata("http://foo/baz.xml",
                            "/tmp/foo/baz.xml",
                            [self.sitemap_index_mock.url])
        )
        error_message = "'http://foo/bar.xml' is a sitemap index. It cannot be referenced in a sitemap index."
        expected_result.add_error(
            Error("http://foo/bar.xml",
                  SiteMapType.SITEMAP_INDEX,
                  "NotASitemapFile",
                  error_message)
        )
        expected_result.add_success_sitemap_index(
            SitemapIndexMetadata(self.sitemap_index_mock.url, 0, 0)
        )
        self.assertEqual(expected_result, actual_result)
        self.assertEqual(self.expected_download_calls,
                         download_url_mock.mock_calls)
        remove_mock.assert_called_once_with("/tmp/foo/bar.xml")


    @mock.patch("os.remove", autospec=True)
    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    def test_invalid_file(self,
                          instanciate_sitemap_document_mock,
                          download_url_mock,
                          remove_mock):
        instanciate_sitemap_document_mock.side_effect = iter([
            self.unknown_sitemap_mock,
            self.sitemap_mock
        ])

        actual_result = Metadata()
        download_sitemaps_from_sitemap_index(self.sitemap_index_mock,
                                             self.output_dir,
                                             self.user_agent,
                                             actual_result)

        expected_result = Metadata()
        expected_result.add_success_sitemap(
            SitemapMetadata("http://foo/baz.xml",
                            "/tmp/foo/baz.xml",
                            [self.sitemap_index_mock.url])
        )
        error_message = "'http://foo/bar.xml' is not a sitemap file."
        expected_result.add_error(
            Error("http://foo/bar.xml",
                  SiteMapType.UNKNOWN,
                  "UnhandledFileType",
                  error_message)
        )
        expected_result.add_success_sitemap_index(
            SitemapIndexMetadata(self.sitemap_index_mock.url, 0, 0)
        )
        self.assertEqual(expected_result, actual_result)
        self.assertEqual(self.expected_download_calls,
                         download_url_mock.mock_calls)
        remove_mock.assert_called_once_with("/tmp/foo/bar.xml")


    @mock.patch("os.remove", autospec=True)
    @mock.patch("os.path.isfile", autospec=True)
    @mock.patch("cdf.features.sitemap.download.download_url")
    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    def test_download_error(self,
                            instanciate_sitemap_document_mock,
                            download_url_mock,
                            is_file_mock,
                            remove_mock):
        download_url_mock.side_effect = [DownloadError("error message"), "/tmp/foo/baz.xml"]

        instanciate_sitemap_document_mock.return_value = self.sitemap_mock

        is_file_mock.return_value = True

        actual_result = Metadata()
        download_sitemaps_from_sitemap_index(self.sitemap_index_mock,
                                             self.output_dir,
                                             self.user_agent,
                                             actual_result)

        expected_result = Metadata()
        expected_result.add_error(
            Error("http://foo/bar.xml",
                  SiteMapType.UNKNOWN,
                  "DownloadError",
                  "error message")
        )
        expected_result.add_success_sitemap(
            SitemapMetadata("http://foo/baz.xml", "/tmp/foo/baz.xml", [self.sitemap_index_mock.url])
        )
        #0 valid urls because we're mocking get_urls() which is supposed to
        #increment the valid url count
        expected_result.add_success_sitemap_index(SitemapIndexMetadata(self.sitemap_index_mock.url, 0, 0))
        self.assertEqual(expected_result, actual_result)
        self.assertEqual(self.expected_download_calls,
                         download_url_mock.mock_calls)
        remove_mock.assert_called_once_with("/tmp/foo/bar.xml")

    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    def test_xml_parsing_error_url_generator(self,
                                             download_url_mock,
                                             instanciate_sitemap_document_mock):
        download_url_mock.return_value = "/tmp/foo/bar.xml"
        instanciate_sitemap_document_mock.return_value = self.sitemap_mock

        def url_generator():
            self.sitemap_index_mock.valid_urls += 1
            yield "http://foo/bar.xml"
            raise ParsingError("error message")
        self.sitemap_index_mock.get_urls = mock.MagicMock()
        self.sitemap_index_mock.get_urls = url_generator

        actual_result = Metadata()
        download_sitemaps_from_sitemap_index(
            self.sitemap_index_mock,
            self.output_dir,
            self.user_agent,
            actual_result)
        expected_result = Metadata()
        expected_result.add_success_sitemap(
            SitemapMetadata("http://foo/bar.xml", "/tmp/foo/bar.xml", [self.sitemap_index_mock.url])
        )
        sitemap_index_metadata = SitemapIndexMetadata(self.sitemap_index_mock.url, 1, 0)
        sitemap_index_metadata.error_type = "ParsingError"
        sitemap_index_metadata.error_message = "error message"
        expected_result.add_success_sitemap_index(sitemap_index_metadata)
        self.assertEqual(expected_result, actual_result)


    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    def test_xml_parsing_error_url_generator_nothing_valid(self,
                                                           download_url_mock,
                                                           instanciate_sitemap_document_mock):
        download_url_mock.return_value = "/tmp/foo/bar.xml"
        instanciate_sitemap_document_mock.return_value = self.sitemap_mock

        def url_generator():
            raise ParsingError("Fake error")
            yield "http://foo"
        self.sitemap_index_mock.get_urls = mock.MagicMock()
        self.sitemap_index_mock.get_urls = url_generator

        actual_result = Metadata()
        download_sitemaps_from_sitemap_index(
            self.sitemap_index_mock,
            self.output_dir,
            self.user_agent,
            actual_result)
        expected_result = Metadata()
        expected_result.add_error(
            Error(self.sitemap_index_mock.url,
                  SiteMapType.SITEMAP_INDEX,
                  "ParsingError",
                  "Fake error")
        )
        self.assertEqual(expected_result, actual_result)

    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    def test_already_processed_url(self,
                                   download_url_mock,
                                   instanciate_sitemap_document_mock):
        sitemap_1_mock = SitemapXmlDocument("/tmp/foo/bar.xml", "http://foo/bar.xml")
        sitemap_2_mock = SitemapXmlDocument("/tmp/foo/baz.xml", "http://foo/baz.xml")
        instanciate_sitemap_document_mock.side_effect = iter([
            sitemap_1_mock,
            sitemap_2_mock
        ])

        actual_result = Metadata()
        #http://foo/bar.xml has already been downloaded
        a_sitemap_index_url = "http://foo.com/a_sitemap_index.xml"
        actual_result.add_success_sitemap(
            SitemapMetadata("http://foo/bar.xml", "/tmp/foo/bar.xml", [a_sitemap_index_url])
        )


        download_sitemaps_from_sitemap_index(self.sitemap_index_mock,
                                             self.output_dir,
                                             self.user_agent,
                                             actual_result)

        expected_result = Metadata()
        expected_result.add_success_sitemap(
            SitemapMetadata("http://foo/bar.xml",
                            "/tmp/foo/bar.xml",
                            #now we have two reference sitemap indexes.
                            [a_sitemap_index_url, self.sitemap_index_mock.url])
        )
        expected_result.add_success_sitemap(
            SitemapMetadata("http://foo/baz.xml",
                            "/tmp/foo/baz.xml",
                            [self.sitemap_index_mock.url])
        )
        expected_result.add_success_sitemap_index(
            SitemapIndexMetadata(self.sitemap_index_mock.url, 0, 0)
        )
        self.assertEqual(expected_result, actual_result)
        download_url_mock.assert_called_once_with("http://foo/baz.xml",
                                                  "/tmp/foo/baz.xml",
                                                  self.user_agent)

