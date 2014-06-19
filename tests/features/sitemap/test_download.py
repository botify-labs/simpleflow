import unittest
import mock
import json
from cdf.features.sitemap.download import (DownloadStatus,
                                           Sitemap,
                                           download_sitemaps,
                                           download_sitemaps_from_urls,
                                           get_output_file_path)

from cdf.features.sitemap.document import SiteMapType, SitemapDocument
from cdf.features.sitemap.exceptions import (DownloadError,
                                             ParsingError,
                                             UnhandledFileType)

class TestDownloadStatus(unittest.TestCase):
    def test_to_json(self):
        download_status = DownloadStatus(
            [Sitemap("http://foo/sitemap_1.xml", "s3://foo/sitemap_1.xml")],
            ["http://error1", "http://error2"]
        )

        actual_result = download_status.to_json()

        expected_result = {
            "sitemaps": [
                {
                    "url": "http://foo/sitemap_1.xml",
                    "s3_uri": "s3://foo/sitemap_1.xml"
                }
            ],
            "errors": [
                "http://error1",
                "http://error2"
            ]
        }
        #compare the objects instead of the json representation
        #to be insensitive to item ordering
        self.assertEqual(expected_result, json.loads(actual_result))

    def test_update(self):
        download_status = DownloadStatus(
            [Sitemap("http://foo/sitemap_1.xml", "s3://foo/sitemap_1.xml")],
            ["http://error1", "http://error2"]
        )

        download_status_aux = DownloadStatus(
            [Sitemap("http://foo/sitemap_2.xml", "s3://foo/sitemap_2.xml")],
            ["http://error3"]
        )

        download_status.update(download_status_aux)
        expected_result = DownloadStatus(
            [
                Sitemap("http://foo/sitemap_1.xml", "s3://foo/sitemap_1.xml"),
                Sitemap("http://foo/sitemap_2.xml", "s3://foo/sitemap_2.xml")
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

    def tearDown(self):
        pass

    @mock.patch("cdf.features.sitemap.download.download_url")
    @mock.patch.object(SitemapDocument, "get_sitemap_type")
    def test_sitemap_case(self,
                          get_sitemap_type_mock,
                          download_url_mock):
        get_sitemap_type_mock.return_value = SiteMapType.SITEMAP

        actual_result = download_sitemaps(self.sitemap_url, self.output_dir)
        expected_result = DownloadStatus()
        expected_result.add_sitemap(Sitemap(self.sitemap_url, "/tmp/foo/sitemap.xml"))
        self.assertEqual(expected_result, actual_result)
        download_url_mock.assert_called_once_with(self.sitemap_url,
                                                  "/tmp/foo/sitemap.xml")


    @mock.patch("os.remove")
    @mock.patch("cdf.features.sitemap.download.download_url")
    @mock.patch("cdf.features.sitemap.download.download_sitemaps_from_urls")
    @mock.patch.object(SitemapDocument, "get_urls")
    @mock.patch.object(SitemapDocument, "get_sitemap_type")
    def test_sitemap_index_case(self,
                                get_sitemap_type_mock,
                                get_urls_mock,
                                download_sitemaps_from_urls_mock,
                                download_url_mock,
                                remove_mock):
        get_sitemap_type_mock.return_value = SiteMapType.SITEMAP_INDEX
        get_urls_mock.return_value = iter(self.sitemap_url)

        download_sitemaps_from_urls_mock.return_value = {
            self.sitemap_url: "/tmp/foo/sitemap.xml"
        }

        input_url = self.sitemap_index_url
        actual_result = download_sitemaps(input_url, self.output_dir)
        expected_result = {self.sitemap_url: "/tmp/foo/sitemap.xml"}
        self.assertEqual(expected_result, actual_result)
        download_url_mock.assert_called_once_with(self.sitemap_index_url,
                                                  "/tmp/foo/sitemap_index.xml")
        download_sitemaps_from_urls_mock.assert_called_once()
        remove_mock.assert_called_once_with("/tmp/foo/sitemap_index.xml")

    @mock.patch("cdf.features.sitemap.download.download_url", new=mock.MagicMock())
    @mock.patch.object(SitemapDocument, "get_sitemap_type")
    def test_not_sitemap_file(self,
                              get_sitemap_type_mock):
        get_sitemap_type_mock.return_value = SiteMapType.UNKNOWN
        input_url = "http://foo/bar.xml"
        self.assertRaises(
            UnhandledFileType,
            download_sitemaps,
            input_url,
            self.output_dir)

    @mock.patch("cdf.features.sitemap.download.download_url")
    def test_download_error(self,
                            download_url_mock):
        download_url_mock.side_effect = DownloadError("foo")

        actual_result = download_sitemaps(self.sitemap_url, self.output_dir)
        expected_result = DownloadStatus()
        expected_result.add_error(self.sitemap_url)
        self.assertEqual(expected_result, actual_result)


    @mock.patch("os.remove")
    @mock.patch("cdf.features.sitemap.download.download_url", new=mock.MagicMock())
    @mock.patch.object(SitemapDocument, "get_urls")
    @mock.patch.object(SitemapDocument, "get_sitemap_type")
    def test_parsing_error(self,
                           get_sitemap_type_mock,
                           get_urls_mock,
                           remove_mock):
        get_sitemap_type_mock.return_value = SiteMapType.SITEMAP_INDEX
        def url_generator():
            raise ParsingError()
            yield "http://foo.com"
        get_urls_mock.return_value = url_generator()
        actual_result = download_sitemaps(self.sitemap_index_url, self.output_dir)
        expected_result = DownloadStatus()
        expected_result.add_error(self.sitemap_index_url)
        self.assertEqual(expected_result, actual_result)
        remove_mock.assert_called_once_with("/tmp/foo/sitemap_index.xml")


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
    @mock.patch.object(SitemapDocument, "get_sitemap_type")
    def test_nominal_case(self,
                          sitemap_type_mock,
                          download_url_mock):
        sitemap_type_mock.return_value = SiteMapType.SITEMAP

        actual_result = download_sitemaps_from_urls(self.urls, self.output_dir)

        expected_result = DownloadStatus()
        expected_result.add_sitemap(Sitemap("http://foo/bar.xml", "/tmp/foo/bar.xml"))
        expected_result.add_sitemap(Sitemap("http://foo/baz.xml", "/tmp/foo/baz.xml"))

        self.assertEqual(expected_result, actual_result)
        self.assertEqual(self.expected_download_calls,
                         download_url_mock.mock_calls)

    def test_empty_list(self):
        actual_result = download_sitemaps_from_urls([], self.output_dir)
        self.assertEqual(DownloadStatus(), actual_result)

    @mock.patch("os.remove")
    @mock.patch("cdf.features.sitemap.download.download_url")
    @mock.patch.object(SitemapDocument, "get_sitemap_type")
    def test_invalid_file(self,
                          sitemap_type_mock,
                          download_url_mock,
                          remove_mock):
        sitemap_type_mock.side_effect = [
            SiteMapType.SITEMAP_INDEX,  # invalid doc
            SiteMapType.SITEMAP
        ]

        actual_result = download_sitemaps_from_urls(self.urls, self.output_dir)

        expected_result = DownloadStatus()
        expected_result.add_sitemap(Sitemap("http://foo/baz.xml", "/tmp/foo/baz.xml"))
        self.assertEqual(expected_result, actual_result)
        self.assertEqual(self.expected_download_calls,
                         download_url_mock.mock_calls)
        remove_mock.assert_called_once_with("/tmp/foo/bar.xml")

    @mock.patch("os.remove")
    @mock.patch("os.path.isfile")
    @mock.patch("cdf.features.sitemap.download.download_url")
    @mock.patch.object(SitemapDocument, "get_sitemap_type")
    def test_download_error(self,
                            sitemap_type_mock,
                            download_url_mock,
                            is_file_mock,
                            remove_mock):
        download_url_mock.side_effect = [DownloadError, "/tmp/foo/baz.xml"]

        sitemap_type_mock.return_value = SiteMapType.SITEMAP

        is_file_mock.return_value = True

        actual_result = download_sitemaps_from_urls(self.urls, self.output_dir)
        expected_result = DownloadStatus()
        expected_result.add_error("http://foo/bar.xml")
        expected_result.add_sitemap(
            Sitemap("http://foo/baz.xml", "/tmp/foo/baz.xml")
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
            self.output_dir)
