import unittest
import mock

from cdf.features.sitemap.download import Sitemap, DownloadStatus
from cdf.features.sitemap.tasks import (download_sitemap_files,
                                        download_sitemap_file)


class TestDownloadSitemapFiles(unittest.TestCase):
    @mock.patch('cdf.utils.s3.push_content',)
    @mock.patch('cdf.features.sitemap.tasks.download_sitemap_file')
    def test_nominal_case(self,
                          download_sitemap_file_mock,
                          push_content_mock):
        sitemap_index = "http://foo.com/sitemap_index.xml"
        #mocking
        download_sitemap_file_mock.side_effect = [
            DownloadStatus([Sitemap("http://foo.com/sitemap.xml",
                                    "s3://foo/sitemaps/sitemap.xml",
                                    sitemap_index)]),
            DownloadStatus([Sitemap("http://bar.com/sitemap.xml",
                                    "s3://foo/sitemaps/sitemap.xml_2",
                                    sitemap_index)])
        ]

        #actual call
        input_urls = [
            "http://foo.com/sitemap.xml",
            "http://bar.com/sitemap.xml"
        ]
        s3_uri = "s3://foo"
        download_sitemap_files(input_urls, s3_uri)

        #verifications
        expected_download_status = DownloadStatus([
            Sitemap("http://foo.com/sitemap.xml",
                    "s3://foo/sitemaps/sitemap.xml",
                    sitemap_index),
            Sitemap("http://bar.com/sitemap.xml",
                    "s3://foo/sitemaps/sitemap.xml_2",
                    sitemap_index)
        ])

        push_content_mock.assert_called_once_with(
            "s3://foo/sitemaps/download_status.json",
            expected_download_status.to_json()
        )


class TestDownloadSitemapFile(unittest.TestCase):
    @mock.patch('cdf.utils.s3.push_file')
    @mock.patch('cdf.features.sitemap.tasks.download_sitemaps')
    def test_nominal_case(self,
                          download_sitemaps_mock,
                          push_file_mock):
        #mocking
        download_sitemaps_mock.return_value = DownloadStatus(
            [Sitemap("http://foo.com/sitemap.xml", "/tmp/foo/sitemap.xml", None)]
        )

        #actual call
        input_url = "http://foo.com/sitemap.xml"
        s3_uri = "s3://foo"
        actual_result = download_sitemap_file(input_url, s3_uri, None)

        #verifications
        expected_result = DownloadStatus([
            Sitemap("http://foo.com/sitemap.xml", "s3://foo/sitemaps/sitemap.xml", None)
        ])
        self.assertEqual(expected_result, actual_result)

        push_file_mock.assert_called_once_with(
            "s3://foo/sitemaps/sitemap.xml",
            "/tmp/foo/sitemap.xml"
        )
