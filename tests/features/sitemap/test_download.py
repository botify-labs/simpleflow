import unittest
import mock
import os
import json
from cdf.features.sitemap.download import (DownloadStatus,
                                           Error,
                                           Sitemap,
                                           download_sitemaps,
                                           download_sitemaps_from_urls,
                                           get_output_file_path)

from cdf.features.sitemap.document import (SiteMapType,
                                           SitemapXmlDocument,
                                           SitemapRssDocument)
from cdf.features.sitemap.exceptions import (DownloadError,
                                             ParsingError,
                                             UnhandledFileType)

class TestDownloadStatus(unittest.TestCase):
    def setUp(self):
        self.sitemap_index = "http://foo/sitemap_index.xml"

    def test_to_json(self):
        download_status = DownloadStatus(
            [Sitemap("http://foo/sitemap_1.xml",
                     "s3://foo/sitemap_1.xml",
                     self.sitemap_index)],
            [Error("http://error1", "foo", "bar"), Error("http://error2", "foo", "bar")]
        )

        actual_result = download_status.to_json()

        expected_result = {
            "sitemaps": [
                {
                    "url": "http://foo/sitemap_1.xml",
                    "s3_uri": "s3://foo/sitemap_1.xml",
                    "sitemap_index": "http://foo/sitemap_index.xml"
                }
            ],
            "errors": [
                {
                    "url": "http://error1",
                    "error": "foo",
                    "message": "bar"
                },
                {
                    "url": "http://error2",
                    "error": "foo",
                    "message": "bar"
                }
            ]
        }
        #compare the objects instead of the json representation
        #to be insensitive to item ordering
        self.assertEqual(expected_result, json.loads(actual_result))

    def test_to_json_no_sitemap(self):
        download_status = DownloadStatus(
            [Sitemap("http://foo/sitemap_1.xml",
                     "s3://foo/sitemap_1.xml",
                     None)]
        )

        actual_result = download_status.to_json()

        expected_result = {
            "sitemaps": [
                {
                    "url": u"http://foo/sitemap_1.xml",
                    "s3_uri": u"s3://foo/sitemap_1.xml",
                    "sitemap_index": None
                },
            ],
            "errors": []
        }
        #compare the objects instead of the json representation
        #to be insensitive to item ordering
        self.assertEqual(expected_result, json.loads(actual_result))

    def test_update(self):

        download_status = DownloadStatus(
            [Sitemap("http://foo/sitemap_1.xml",
                     "s3://foo/sitemap_1.xml",
                     self.sitemap_index)],
            ["http://error1", "http://error2"]
        )

        download_status_aux = DownloadStatus(
            [Sitemap("http://foo/sitemap_2.xml",
                     "s3://foo/sitemap_2.xml",
                     self.sitemap_index)],
            ["http://error3"]
        )

        download_status.update(download_status_aux)
        expected_result = DownloadStatus(
            [
                Sitemap("http://foo/sitemap_1.xml",
                        "s3://foo/sitemap_1.xml",
                        self.sitemap_index),
                Sitemap("http://foo/sitemap_2.xml",
                        "s3://foo/sitemap_2.xml",
                        self.sitemap_index)
            ],
            ["http://error1", "http://error2", "http://error3"]
        )
        self.assertEqual(expected_result, download_status)


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

        self.sitemap_index_mock = mock.create_autospec(SitemapXmlDocument)
        self.sitemap_index_mock.get_sitemap_type.return_value = SiteMapType.SITEMAP_INDEX

    def tearDown(self):
        pass

    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    def test_sitemap_case(self,
                          instanciate_sitemap_document_mock,
                          download_url_mock):
        instanciate_sitemap_document_mock.return_value = self.sitemap_mock

        actual_result = download_sitemaps(self.sitemap_url,
                                          self.output_dir,
                                          self.user_agent)
        expected_result = DownloadStatus()
        expected_result.add_success_sitemap(Sitemap(self.sitemap_url, "/tmp/foo/sitemap.xml", None))
        self.assertEqual(expected_result, actual_result)
        download_url_mock.assert_called_once_with(self.sitemap_url,
                                                  "/tmp/foo/sitemap.xml",
                                                  self.user_agent)


    @mock.patch("os.remove", autospec=True)
    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    @mock.patch("cdf.features.sitemap.download.download_sitemaps_from_urls", autospec=True)
    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    def test_sitemap_index_case(self,
                                instanciate_sitemap_document_mock,
                                download_sitemaps_from_urls_mock,
                                download_url_mock,
                                remove_mock):
        self.sitemap_index_mock.get_urls.return_value = iter(self.sitemap_url)
        instanciate_sitemap_document_mock.return_value = self.sitemap_index_mock

        download_sitemaps_from_urls_mock.return_value = {
            self.sitemap_url: "/tmp/foo/sitemap.xml"
        }

        input_url = self.sitemap_index_url
        actual_result = download_sitemaps(input_url,
                                          self.output_dir,
                                          self.user_agent)
        expected_result = {self.sitemap_url: "/tmp/foo/sitemap.xml"}
        self.assertEqual(expected_result, actual_result)
        download_url_mock.assert_called_once_with(self.sitemap_index_url,
                                                  "/tmp/foo/sitemap_index.xml",
                                                  self.user_agent)
        self.assertEqual(1, download_sitemaps_from_urls_mock.call_count)
        remove_mock.assert_called_once_with("/tmp/foo/sitemap_index.xml")

    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    def test_not_sitemap_file(self,
                              instanciate_sitemap_document_mock,
                              download_url_mock):
        instanciate_sitemap_document_mock.side_effect = UnhandledFileType()
        input_url = "http://foo/bar.xml"
        self.assertRaises(
            UnhandledFileType,
            download_sitemaps,
            input_url,
            self.output_dir,
            self.user_agent)
        download_url_mock.assert_called_once_with("http://foo/bar.xml",
                                                  "/tmp/foo/bar.xml",
                                                  self.user_agent)

    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    def test_download_error(self,
                            download_url_mock):
        download_url_mock.side_effect = DownloadError("foo")

        actual_result = download_sitemaps(self.sitemap_url,
                                          self.output_dir,
                                          self.user_agent)
        expected_result = DownloadStatus()
        expected_result.add_error(self.sitemap_url, "DownloadError", "foo")
        self.assertEqual(expected_result, actual_result)


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
        self.sitemap_index_mock.get_urls.return_value = url_generator()
        instanciate_sitemap_document_mock.return_value = self.sitemap_index_mock

        actual_result = download_sitemaps(self.sitemap_index_url,
                                          self.output_dir,
                                          self.user_agent)
        expected_result = DownloadStatus()
        expected_result.add_error(self.sitemap_index_url, "ParsingError", "error message")
        self.assertEqual(expected_result, actual_result)
        remove_mock.assert_called_once_with("/tmp/foo/sitemap_index.xml")
        download_url_mock.assert_called_once_with(self.sitemap_index_url,
                                                  os.path.join(self.output_dir, "sitemap_index.xml"),
                                                  self.user_agent)


class TestDownloadSitemapsFromUrls(unittest.TestCase):
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

        self.sitemap_index = "http://foo/sitemap_index.xml"

        self.user_agent = "custom user-agent"

        self.sitemap_mock = mock.create_autospec(SitemapXmlDocument)
        self.sitemap_mock.get_sitemap_type.return_value = SiteMapType.SITEMAP_XML

        self.sitemap_index_mock = mock.create_autospec(SitemapXmlDocument)
        self.sitemap_index_mock.get_sitemap_type.return_value = SiteMapType.SITEMAP_INDEX

        self.unknown_sitemap_mock = mock.create_autospec(SitemapXmlDocument)
        self.unknown_sitemap_mock.get_sitemap_type.return_value = SiteMapType.UNKNOWN

    @mock.patch("cdf.features.sitemap.download.download_url", autospec=True)
    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    def test_nominal_case(self,
                          instanciate_sitemap_document_mock,
                          download_url_mock):
        instanciate_sitemap_document_mock.return_value = self.sitemap_mock

        actual_result = download_sitemaps_from_urls(self.urls,
                                                    self.output_dir,
                                                    self.user_agent,
                                                    self.sitemap_index)
        expected_result = DownloadStatus()
        expected_result.add_success_sitemap(Sitemap("http://foo/bar.xml",
                                                    "/tmp/foo/bar.xml",
                                                    self.sitemap_index))
        expected_result.add_success_sitemap(Sitemap("http://foo/baz.xml",
                                                    "/tmp/foo/baz.xml",
                                                    self.sitemap_index))

        self.assertEqual(expected_result, actual_result)
        self.assertEqual(self.expected_download_calls,
                         download_url_mock.mock_calls)

    def test_empty_list(self):
        actual_result = download_sitemaps_from_urls([],
                                                    self.output_dir,
                                                    self.user_agent)
        self.assertEqual(DownloadStatus(), actual_result)

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

        actual_result = download_sitemaps_from_urls(self.urls,
                                                    self.output_dir,
                                                    self.user_agent,
                                                    self.sitemap_index)

        expected_result = DownloadStatus()
        expected_result.add_success_sitemap(Sitemap("http://foo/baz.xml",
                                                    "/tmp/foo/baz.xml",
                                                    self.sitemap_index))
        error_message = "'http://foo/bar.xml' is a sitemap index. It cannot be referenced in a sitemap index."
        expected_result.add_error("http://foo/bar.xml", "NotASitemapFile", error_message)
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

        actual_result = download_sitemaps_from_urls(self.urls,
                                                    self.output_dir,
                                                    self.user_agent,
                                                    self.sitemap_index)

        expected_result = DownloadStatus()
        expected_result.add_success_sitemap(Sitemap("http://foo/baz.xml",
                                                    "/tmp/foo/baz.xml",
                                                    self.sitemap_index))
        error_message = "'http://foo/bar.xml' is not a sitemap file."
        expected_result.add_error("http://foo/bar.xml", "UnhandledFileType", error_message)
        self.assertEqual(expected_result, actual_result)
        self.assertEqual(self.expected_download_calls,
                         download_url_mock.mock_calls)
        remove_mock.assert_called_once_with("/tmp/foo/bar.xml")


    @mock.patch("os.remove", autospec=True)
    @mock.patch("os.path.isfile", autospec=True)
    @mock.patch("cdf.features.sitemap.download.download_url")
    @mock.patch.object(SitemapXmlDocument, "get_sitemap_type", autospec=True)
    @mock.patch("cdf.features.sitemap.download.instanciate_sitemap_document", autospec=True)
    def test_download_error(self,
                            instanciate_sitemap_document_mock,
                            sitemap_type_mock,
                            download_url_mock,
                            is_file_mock,
                            remove_mock):
        download_url_mock.side_effect = [DownloadError("error message"), "/tmp/foo/baz.xml"]

        instanciate_sitemap_document_mock.return_value = self.sitemap_mock

        is_file_mock.return_value = True

        actual_result = download_sitemaps_from_urls(self.urls,
                                                    self.output_dir,
                                                    self.user_agent,
                                                    self.sitemap_index)

        expected_result = DownloadStatus()
        expected_result.add_error("http://foo/bar.xml", "DownloadError", "error message")
        expected_result.add_success_sitemap(
            Sitemap("http://foo/baz.xml", "/tmp/foo/baz.xml", self.sitemap_index)
        )
        self.assertEqual(expected_result, actual_result)
        self.assertEqual(self.expected_download_calls,
                         download_url_mock.mock_calls)
        remove_mock.assert_called_once_with("/tmp/foo/bar.xml")

    def test_xml_parsing_error_url_generator(self):
        def url_generator():
            raise ParsingError()
            yield "http://foo.com"

        self.assertRaises(
            ParsingError,
            download_sitemaps_from_urls,
            url_generator(),
            self.output_dir,
            self.user_agent)
