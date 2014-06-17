import unittest
import mock
import json

from cdf.features.sitemap.tasks import download_sitemap_files


class TestDownloadSitemapFiles(unittest.TestCase):
    @mock.patch('cdf.utils.s3.push_content',)
    @mock.patch('cdf.utils.s3.push_file')
    @mock.patch('cdf.features.sitemap.tasks.download_sitemaps')
    def test_nominal_case(self,
                          download_sitemap_files_mock,
                          push_file_mock,
                          push_content_mock):
        #mocking
        download_sitemap_files_mock.side_effect = [
            {"http://foo.com/sitemap.xml": "/tmp/foo/sitemap.xml"},
            {"http://bar.com/sitemap.xml": "/tmp/foo/sitemap.xml_2"},
        ]

        #actual call
        input_urls = [
            "http://foo.com/sitemap.xml",
            "http://bar.com/sitemap.xml"
        ]
        s3_uri = "s3://foo"
        download_sitemap_files(input_urls, s3_uri)

        #verifications
        self.assertItemsEqual(
            [mock.call("s3://foo/sitemaps/sitemap.xml", "/tmp/foo/sitemap.xml"),
             mock.call("s3://foo/sitemaps/sitemap.xml_2", "/tmp/foo/sitemap.xml_2")],
            push_file_mock.mock_calls)

        expected_file_index = {
            "http://foo.com/sitemap.xml": "s3://foo/sitemaps/sitemap.xml",
            "http://bar.com/sitemap.xml": "s3://foo/sitemaps/sitemap.xml_2",
        }
        push_content_mock.assert_called_once_with(
            "s3://foo/sitemaps/file_index.json", json.dumps(expected_file_index)
        )
