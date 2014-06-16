import unittest
import mock

from cdf.features.sitemap.download import (download_sitemaps,
                                           download_sitemaps_from_urls,
                                           get_output_file_path)

from cdf.features.sitemap.document import SiteMapDocument, SiteMapType
from cdf.features.sitemap.exceptions import (ParsingError,
                                             DownloadError,
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

    @mock.patch("cdf.features.sitemap.download.download_url")
    @mock.patch("cdf.features.sitemap.download.parse_sitemap_file")
    def test_sitemap_case(self, parse_sitemap_file_mock, download_url_mock):
        parse_sitemap_file_mock.return_value = SiteMapDocument(SiteMapType.SITEMAP, None)

        actual_result = download_sitemaps(self.sitemap_url, self.output_dir)
        expected_result = {self.sitemap_url: "/tmp/foo/sitemap.xml"}
        self.assertEqual(expected_result, actual_result)
        download_url_mock.assert_called_once_with(self.sitemap_url, "/tmp/foo/sitemap.xml")

    @mock.patch("os.remove")
    @mock.patch("cdf.features.sitemap.download.download_url")
    @mock.patch("cdf.features.sitemap.download.download_sitemaps_from_urls")
    @mock.patch("cdf.features.sitemap.download.parse_sitemap_file")
    def test_sitemap_index_case(self,
                                parse_sitemap_file_mock,
                                download_sitemaps_from_urls_mock,
                                download_url_mock,
                                remove_mock):
        sitemap_index_mock = SiteMapDocument(SiteMapType.SITEMAP_INDEX, None)
        sitemap_index_mock.get_urls = mock.MagicMock()
        sitemap_index_mock.get_urls.return_value = [self.sitemap_url]
        parse_sitemap_file_mock.return_value = sitemap_index_mock

        download_sitemaps_from_urls_mock.return_value = {self.sitemap_url: "/tmp/foo/sitemap.xml"}

        input_url = self.sitemap_index_url
        actual_result = download_sitemaps(input_url, self.output_dir)
        expected_result = {self.sitemap_url: "/tmp/foo/sitemap.xml"}
        self.assertEqual(expected_result, actual_result)
        download_url_mock.assert_called_once_with(self.sitemap_index_url,
                                                  "/tmp/foo/sitemap_index.xml")
        download_sitemaps_from_urls_mock.assert_called_once_with([self.sitemap_url],
                                                                 self.output_dir)
        remove_mock.assert_called_once_with("/tmp/foo/sitemap_index.xml")

    @mock.patch("cdf.features.sitemap.download.download_url")
    @mock.patch("cdf.features.sitemap.download.parse_sitemap_file")
    def test_invalid_xml(self, parse_sitemap_file_mock, download_url_mock):
        parse_sitemap_file_mock.side_effect = ParsingError("error")

        input_url = "http://foo/bar.xml"
        actual_result = download_sitemaps(input_url, self.output_dir)
        self.assertEqual({}, actual_result)


    @mock.patch("cdf.features.sitemap.download.download_url")
    @mock.patch("cdf.features.sitemap.download.parse_sitemap_file")
    def test_not_sitemap_file(self, parse_sitemap_file_mock, download_url_mock):
        parse_sitemap_file_mock.return_value = SiteMapDocument(None, None)
        input_url = "http://foo/bar.xml"
        self.assertRaises(
            UnhandledFileType,
            download_sitemaps,
            input_url,
            self.output_dir)


class TestDownloadSitemapsFromUrls(unittest.TestCase):
    def setUp(self):
        self.urls = [
            "http://foo/bar.xml",
            "http://foo/baz.xml"
        ]

        self.output_dir = "/tmp/foo"

        self.expected_download_calls = [
            mock.call("http://foo/bar.xml", "/tmp/foo/bar.xml"),
            mock.call("http://foo/baz.xml", "/tmp/foo/baz.xml")
        ]

    @mock.patch("cdf.features.sitemap.download.download_url")
    @mock.patch("cdf.features.sitemap.download.parse_sitemap_file")
    def test_nominal_case(self, parse_sitemap_file_mock, download_url_mock):
        parse_sitemap_file_mock.return_value = SiteMapDocument(SiteMapType.SITEMAP, None)

        actual_result = download_sitemaps_from_urls(self.urls, self.output_dir)

        expected_result = {
            "http://foo/bar.xml": "/tmp/foo/bar.xml",
            "http://foo/baz.xml": "/tmp/foo/baz.xml"
        }
        self.assertEqual(expected_result, actual_result)
        self.assertEqual(self.expected_download_calls,
                         download_url_mock.mock_calls)

    def test_empty_list(self):
        actual_result = download_sitemaps_from_urls([], self.output_dir)
        self.assertEqual({}, actual_result)

    @mock.patch("os.remove")
    @mock.patch("cdf.features.sitemap.download.download_url")
    @mock.patch("cdf.features.sitemap.download.parse_sitemap_file")
    def test_invalid_file(self, parse_sitemap_file_mock, download_url_mock,
                          remove_mock):
        parse_sitemap_file_mock.side_effect = [
            SiteMapDocument(SiteMapType.SITEMAP_INDEX, None),  # invalid doc
            SiteMapDocument(SiteMapType.SITEMAP, None),
        ]

        actual_result = download_sitemaps_from_urls(self.urls, self.output_dir)

        expected_result = {
            "http://foo/baz.xml": "/tmp/foo/baz.xml"
        }
        self.assertEqual(expected_result, actual_result)
        self.assertEqual(self.expected_download_calls,
                         download_url_mock.mock_calls)
        remove_mock.assert_called_once_with("/tmp/foo/bar.xml")


    @mock.patch("os.remove")
    @mock.patch("os.path.isfile")
    @mock.patch("cdf.features.sitemap.download.download_url")
    @mock.patch("cdf.features.sitemap.download.parse_sitemap_file")
    def test_inv_file(self, parse_sitemap_file_mock, download_url_mock,
                      is_file_mock, remove_mock):
        download_url_mock.side_effect = [DownloadError, "/tmp/foo/baz.xml"]

        parse_sitemap_file_mock.side_effect = [
            SiteMapDocument(SiteMapType.SITEMAP, None),
        ]

        is_file_mock.return_value = True

        actual_result = download_sitemaps_from_urls(self.urls, self.output_dir)

        expected_result = {
            "http://foo/baz.xml": "/tmp/foo/baz.xml"
        }
        self.assertEqual(expected_result, actual_result)
        self.assertEqual(self.expected_download_calls,
                         download_url_mock.mock_calls)
        remove_mock.assert_called_once_with("/tmp/foo/bar.xml")

