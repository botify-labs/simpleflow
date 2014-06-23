import unittest
import mock
import tempfile
import gzip
import os

from cdf.features.sitemap.download import Sitemap, DownloadStatus
from cdf.features.main.streams import IdStreamDef
from cdf.features.sitemap.tasks import (download_sitemap_files,
                                        download_sitemap_file,
                                        match_sitemap_urls)
from cdf.core.mocks import _mock_push_file


class TestDownloadSitemapFiles(unittest.TestCase):
    @mock.patch('cdf.utils.s3.push_content', autospec=True)
    @mock.patch('cdf.features.sitemap.tasks.download_sitemap_file', autospec=True)
    def test_nominal_case(self,
                          download_sitemap_file_mock,
                          push_content_mock):
        sitemap_index = "http://foo.com/sitemap_index.xml"
        #mocking
        download_sitemap_file_mock.side_effect = iter([
            DownloadStatus([Sitemap("http://foo.com/sitemap.xml",
                                    "s3://foo/sitemaps/sitemap.xml",
                                    sitemap_index)]),
            DownloadStatus([Sitemap("http://bar.com/sitemap.xml",
                                    "s3://foo/sitemaps/sitemap.xml_2",
                                    sitemap_index)])
        ])

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
    @mock.patch('cdf.utils.s3.push_file', autospec=True)
    @mock.patch('cdf.features.sitemap.tasks.download_sitemaps', autospec=True)
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


class TestMatchSitemapUrls(unittest.TestCase):
    @mock.patch('cdf.utils.s3.push_file', _mock_push_file)
    @mock.patch("cdf.features.sitemap.tasks.get_sitemap_urls_stream", autospec=True)
    @mock.patch.object(IdStreamDef, 'get_stream_from_s3')
    def test_nominal_case(self,
                          get_stream_from_s3_mock,
                          get_sitemap_urls_stream_mock):
        #mock definition
        get_stream_from_s3_mock.return_value = [
            (1, "http", "foo.com", "/bar", ""),
            (2, "http", "foo.com", "/baz", ""),
            (3, "http", "foo.com", "/qux", ""),
        ]

        get_sitemap_urls_stream_mock.return_value = [
            "http://foo.com/qux",
            "http://foo.com/bar",
            "http://foo.com/index.html"  # not in crawl
        ]

        #call
        s3_uri = "s3://" + tempfile.mkdtemp()
        first_part_id_size = 10
        part_id_size = 100

        match_sitemap_urls(s3_uri,
                           first_part_id_size,
                           part_id_size)

        #check output files
        with gzip.open(os.path.join(s3_uri[5:], 'sitemap_only.gz')) as f:
            expected_result = ['http://foo.com/index.html\n']
            self.assertEquals(expected_result, f.readlines())

        with gzip.open(os.path.join(s3_uri[5:], 'sitemap.txt.0.gz')) as f:
            expected_result = ['1\n', '3\n']  # urlids are now sorted
            self.assertEquals(expected_result, f.readlines())
