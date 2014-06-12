import unittest
import mock

from cdf.features.sitemap.download import (download_sitemaps,
                                           download_sitemaps_from_urls,
                                           get_output_file_path)

from cdf.features.sitemap.document import SiteMapDocument, SiteMapType


class TestGetOutputFilePath(unittest.TestCase):
    def test_no_collision_case(self):
        self.assertEqual("/tmp/foo/sitemap.xml",
                         get_output_file_path("http://bar/sitemap.xml", "/tmp/foo"))

    @mock.patch("os.path.exists")
    def test_collision_case(self, exist_mock):
        def side_effect(arg):
            return arg in ["/tmp/foo/sitemap.xml", "/tmp/foo/sitemap.xml_2"]
        exist_mock.side_effect = side_effect
        url = "http://bar/sitemap.xml"
        output_directory = "/tmp/foo"
        self.assertEqual("/tmp/foo/sitemap.xml_3",
                         get_output_file_path(url, output_directory))


class TestDownloadSiteMaps(unittest.TestCase):
    @mock.patch("cdf.features.sitemap.download.download_url")
    @mock.patch("cdf.features.sitemap.download.parse_sitemap_file")
    def test_sitemap_case(self, parse_sitemap_file_mock, download_url_mock):
        parse_sitemap_file_mock.return_value = SiteMapDocument(SiteMapType.SITEMAP, None)

        input_url = "http://foo/bar.xml"
        output_directory = "/tmp/foo"
        actual_result = download_sitemaps(input_url, output_directory)
        expected_result = {"http://foo/bar.xml": "/tmp/foo/bar.xml"}
        self.assertEqual(expected_result, actual_result)
        download_url_mock.assert_called_once_with("http://foo/bar.xml", "/tmp/foo/bar.xml")

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
        sitemap_index_mock.get_urls.return_value = ["http://bar/baz.xml"]
        parse_sitemap_file_mock.return_value = sitemap_index_mock

        download_sitemaps_from_urls_mock.return_value = {"http://bar/baz.xml": "/tmp/foo/baz.xml"}

        input_url = "http://foo/bar.xml"
        output_directory = "/tmp/foo"
        actual_result = download_sitemaps(input_url, output_directory)
        expected_result = {"http://bar/baz.xml": "/tmp/foo/baz.xml"}
        self.assertEqual(expected_result, actual_result)
        download_url_mock.assert_called_once_with("http://foo/bar.xml", "/tmp/foo/bar.xml")
        download_sitemaps_from_urls_mock.assert_called_once_with(["http://bar/baz.xml"], "/tmp/foo")
        remove_mock.assert_called_once_with("/tmp/foo/bar.xml")


class TestDownloadSitemapsFromUrls(unittest.TestCase):
    @mock.patch("cdf.features.sitemap.download.download_url")
    @mock.patch("cdf.features.sitemap.download.parse_sitemap_file")
    def test_nominal_case(self, parse_sitemap_file_mock, download_url_mock):
        parse_sitemap_file_mock.return_value = SiteMapDocument(SiteMapType.SITEMAP, None)

        urls = [
            "http://foo/bar.xml",
            "http://foo/baz.xml"
        ]
        output_directory = "/tmp/foo"
        actual_result = download_sitemaps_from_urls(urls, output_directory)

        expected_result = {
            "http://foo/bar.xml": "/tmp/foo/bar.xml",
            "http://foo/baz.xml": "/tmp/foo/baz.xml"
        }
        self.assertEqual(expected_result, actual_result)
        expected_calls = [
            mock.call("http://foo/bar.xml", "/tmp/foo/bar.xml"),
            mock.call("http://foo/baz.xml", "/tmp/foo/baz.xml")
        ]
        self.assertEqual(expected_calls, download_url_mock.mock_calls)

    def test_empty_list(self):
        actual_result = download_sitemaps_from_urls([], "/tmp/foo")
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

        urls = [
            "http://foo/bar.xml",
            "http://foo/baz.xml",
        ]
        output_directory = "/tmp/foo"
        actual_result = download_sitemaps_from_urls(urls, output_directory)

        expected_result = {
            "http://foo/baz.xml": "/tmp/foo/baz.xml"
        }
        self.assertEqual(expected_result, actual_result)
        expected_calls = [
            mock.call("http://foo/bar.xml", "/tmp/foo/bar.xml"),
            mock.call("http://foo/baz.xml", "/tmp/foo/baz.xml")
        ]
        self.assertEqual(expected_calls, download_url_mock.mock_calls)
        remove_mock.assert_called_once_with("/tmp/foo/bar.xml")

    def test_name_collision(self):
        pass
