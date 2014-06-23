import unittest
import mock

import os
from cdf.core.streams.base import TemporaryDataset
from cdf.features.sitemap.document import SitemapDocument
from cdf.features.sitemap.download import Sitemap, DownloadStatus
from cdf.features.sitemap.matching import (get_sitemap_urls_stream,
                                           get_download_status_from_s3,
                                           download_sitemaps_from_s3,
                                           match_sitemap_urls_from_stream)


class GetSitemapUrlsStream(unittest.TestCase):
    @mock.patch.object(SitemapDocument, 'get_urls', autospec=True)
    @mock.patch("cdf.features.sitemap.matching.download_sitemaps_from_s3", autospec=True)
    def test_nominal_case(self,
                          download_sitemaps_from_s3_mock,
                          get_urls_mock):
        get_urls_mock.side_effect = iter([
            iter(["foo", "bar"]),
            iter(["baz", "qux"])
        ])

        download_sitemaps_from_s3_mock.return_value = ["/tmp/foo", "/tmp/bar"]
        s3_uri = "s3://foo"
        tmp_dir = "/tmp/foo"
        force_fetch = True
        actual_result = get_sitemap_urls_stream(s3_uri, tmp_dir, force_fetch)
        self.assertEqual(["foo", "bar", "baz", "qux"], list(actual_result))
        download_sitemaps_from_s3_mock.assert_called_once_with(s3_uri,
                                                               tmp_dir,
                                                               force_fetch)


class TestGetDownloadStatusFromS3(unittest.TestCase):
    @mock.patch('cdf.utils.s3.fetch_file', autospec=True)
    def test_nominal_case(self, fetch_file_mock):
        s3_uri = "s3://foo"
        tmp_dir = "/tmp/foo"
        force_fetch = False
        file_content = ('{'
                        '"sitemaps": ['
                        '    {'
                        '       "url": "http://foo/sitemap_1.xml",'
                        '       "s3_uri": "s3://foo/sitemap_1.xml",'
                        '       "sitemap_index": "http://foo/sitemap_index.html"'
                        '   },'
                        '   {'
                        '       "url": "http://foo/sitemap_2.xml",'
                        '       "s3_uri": "s3://foo/sitemap_2.xml",'
                        '       "sitemap_index": "http://foo/sitemap_index.html"'
                        '   }'
                        '],'
                        '"errors": ['
                        '    "http://error"'
                        ']'
                        '}')

        #mock open()
        with mock.patch("__builtin__.open", mock.mock_open(read_data=file_content)) as m:
            actual_result = get_download_status_from_s3(s3_uri,
                                                        tmp_dir,
                                                        force_fetch)

        #check result
        expected_sitemaps = [
            Sitemap("http://foo/sitemap_1.xml",
                    "s3://foo/sitemap_1.xml",
                    "http://foo/sitemap_index.html"),
            Sitemap("http://foo/sitemap_2.xml",
                    "s3://foo/sitemap_2.xml",
                    "http://foo/sitemap_index.html"),
        ]
        expected_errors = ["http://error"]
        expected_result = DownloadStatus(expected_sitemaps, expected_errors)
        self.assertEqual(expected_result, actual_result)

        #check calls
        m.assert_called_once_with("/tmp/foo/download_status.json")
        fetch_file_mock.assert_called_once_with("s3://foo/sitemaps/download_status.json",
                                                "/tmp/foo/download_status.json",
                                                force_fetch)


class TestDownloadSitemapsFromS3(unittest.TestCase):
    @mock.patch('cdf.features.sitemap.matching.get_download_status_from_s3', autospec=True)
    @mock.patch('cdf.utils.s3.fetch_file', autospec=True)
    def test_nominal_case(self,
                          fetch_file_mock,
                          get_download_status_from_s3_mock):
        #mock
        sitemaps = [
            Sitemap("http://foo.com/sitemap_1.xml", "s3://foo/sitemap_1.xml", None),
            Sitemap("http://foo.com/sitemap_2.xml", "s3://foo/sitemap_2.xml", None)
        ]
        get_download_status_from_s3_mock.return_value = DownloadStatus(sitemaps)

        #actual call
        s3_uri = "s3://foo"
        tmp_dir = "/tmp/foo"
        force_fetch = False
        actual_result = download_sitemaps_from_s3(s3_uri, tmp_dir, force_fetch)

        #check result
        expected_result = [
            os.path.join(tmp_dir, "sitemap_1.xml"),
            os.path.join(tmp_dir, "sitemap_2.xml"),
        ]
        self.assertEqual(expected_result, actual_result)

        #check calls
        expected_fetch_calls = [
            mock.call("s3://foo/sitemap_1.xml", os.path.join(tmp_dir, 'sitemap_1.xml'), force_fetch),
            mock.call("s3://foo/sitemap_2.xml", os.path.join(tmp_dir, 'sitemap_2.xml'), force_fetch)
        ]
        self.assertEqual(expected_fetch_calls, fetch_file_mock.mock_calls)


class MatchSitemapUrlsFromStream(unittest.TestCase):
    def test_nominal_case(self):

        url_to_id = {
            "foo": 0,
            "bar": 2,
            "qux": 5
        }

        url_generator = iter(["foo", "bar", "baz", "qux"])

        dataset = mock.create_autospec(TemporaryDataset)
        dataset = mock.MagicMock()
        sitemap_only_file = mock.create_autospec(file)
        match_sitemap_urls_from_stream(url_generator,
                                       url_to_id,
                                       dataset,
                                       sitemap_only_file)

        expected_dataset_calls = [mock.call(0),
                                  mock.call(2),
                                  mock.call(5)]
        self.assertEquals(expected_dataset_calls, dataset.append.mock_calls)
        sitemap_only_file.write.assert_called_once_with("baz\n")
