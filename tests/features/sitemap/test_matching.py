import unittest
import mock

import os
from cdf.core.streams.base import TemporaryDataset
from cdf.features.sitemap.document import SitemapXmlDocument, SiteMapType
from cdf.features.sitemap.download import Sitemap, Error, DownloadStatus
from cdf.features.sitemap.matching import (get_download_status_from_s3,
                                           download_sitemaps_from_s3,
                                           match_sitemap_urls_from_stream,
                                           DomainValidator)


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
                        '    {'
                        '        "url": "http://error",'
                        '        "error": "DownloadError",'
                        '        "message": "foo"'
                        '    }'
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
        expected_errors = [Error(u"http://error", u"DownloadError", u"foo")]
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
            (os.path.join(tmp_dir, "sitemap_1.xml"), "http://foo.com/sitemap_1.xml"),
            (os.path.join(tmp_dir, "sitemap_2.xml"), "http://foo.com/sitemap_2.xml"),
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

        url_generator = iter(["foo", "bar", "baz", "qux", "donald", "mickey"])

        dataset = mock.create_autospec(TemporaryDataset)
        dataset = mock.MagicMock()

        domain_validator = mock.create_autospec(DomainValidator)
        domain_validator.is_valid.side_effect = [True, False, False]

        sitemap_only_nb_samples = 2  # one url will be skipped
        sitemap_only_urls = []
        out_of_crawl_domain_urls = []
        match_sitemap_urls_from_stream(url_generator,
                                       url_to_id,
                                       dataset,
                                       domain_validator,
                                       sitemap_only_nb_samples,
                                       sitemap_only_urls,
                                       out_of_crawl_domain_urls)
        expected_dataset_calls = [mock.call(0),
                                  mock.call(2),
                                  mock.call(5)]
        self.assertEquals(expected_dataset_calls, dataset.append.mock_calls)

        self.assertEqual(["baz"], sitemap_only_urls)
        self.assertEqual(["donald", "mickey"], out_of_crawl_domain_urls)

    def test_duplicated_sitemap_only_urls(self):

        url_to_id = {}

        url_generator = iter(["foo", "bar", "foo", "baz", "qux"])

        dataset = mock.create_autospec(TemporaryDataset)
        dataset = mock.MagicMock()

        domain_validator = mock.create_autospec(DomainValidator)
        domain_validator.is_valid.return_value = True

        sitemap_only_nb_samples = 3  # one url will be skipped
        sitemap_only_urls = []
        out_of_crawl_domain_urls = []
        match_sitemap_urls_from_stream(url_generator,
                                       url_to_id,
                                       dataset,
                                       domain_validator,
                                       sitemap_only_nb_samples,
                                       sitemap_only_urls,
                                       out_of_crawl_domain_urls)

        self.assertEqual(["foo", "bar", "baz"], sitemap_only_urls)
        self.assertEqual([], out_of_crawl_domain_urls)


class TestDomainValidator(unittest.TestCase):
    def test_empty_allowed_domains(self):
        validator = DomainValidator([])
        self.assertFalse(validator.is_valid("http://wired.com"))

    def test_single_domain(self):
        validator = DomainValidator(["wired.com"])
        self.assertTrue(validator.is_valid("http://wired.com"))
        self.assertTrue(validator.is_valid("https://wired.com"))
        self.assertTrue(validator.is_valid("http://wired.com/gadgets"))

        self.assertFalse(validator.is_valid("http://news.wired.com/googleio"))
        self.assertFalse(validator.is_valid("http://theverge.com/googleio"))

    def test_wildcard(self):
        validator = DomainValidator(["*.wired.com"])
        self.assertTrue(validator.is_valid("http://news.wired.com"))
        self.assertTrue(validator.is_valid("http://.wired.com"))

        self.assertFalse(validator.is_valid("http://wired.com"))
        self.assertFalse(validator.is_valid("http://newswired.com"))

    def test_wildcard_special_characters(self):
        #we want only * as special character
        self.assertFalse(DomainValidator(["wired.?"]).is_valid("http://wired.a"))

        self.assertFalse(DomainValidator(["[a-z].com"]).is_valid("http://a.com"))
        #tested as litteral
        self.assertTrue(DomainValidator(["[a-z].com"]).is_valid("http://[a-z].com"))

    def test_multiple_domains(self):
        validator = DomainValidator(["wired.com", "news.wired.com"])
        self.assertTrue(validator.is_valid("http://wired.com/startups"))
        self.assertTrue(validator.is_valid("http://news.wired.com/googleio"))

        self.assertFalse(validator.is_valid("http://blog.wired.com/googleio"))

    def test_blacklisted_domain(self):
        validator = DomainValidator(["*.wired.com"], ["news.wired.com"])
        self.assertTrue(validator.is_valid("http://blog.wired.com/post"))
        self.assertFalse(validator.is_valid("http://news.wired.com/science"))

    def test_blacklisted_domain_wildcard(self):
        #wildcard are not allowed in blacklisted domains
        validator = DomainValidator(["*.wired.com"], ["*.wired.com"])
        self.assertTrue(validator.is_valid("http://blog.wired.com/post"))
        #check that * is interpreted as a literal
        self.assertFalse(validator.is_valid("http://*.wired.com/post"))
