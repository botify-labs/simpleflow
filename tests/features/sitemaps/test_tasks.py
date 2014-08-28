# -*- coding: utf-8 -*
import unittest
import mock
from moto import mock_s3
import boto
import json
from cdf.utils.s3 import stream_files

from cdf.features.sitemaps.metadata import (SitemapMetadata,
                                           Metadata,
                                           SiteMapType)
from cdf.features.sitemaps.document import SitemapXmlDocument
from cdf.features.main.streams import IdStreamDef
from cdf.features.sitemaps.tasks import (download_sitemap_files,
                                        download_sitemap_file,
                                        match_sitemap_urls,
                                        update_download_status,
                                        save_url_list_as_gzip)


class TestDownloadSitemapFiles(unittest.TestCase):
    @mock.patch('cdf.utils.s3.push_content', autospec=True)
    @mock.patch('cdf.features.sitemaps.tasks.download_sitemap_file', autospec=True)
    def test_nominal_case(self,
                          download_sitemap_file_mock,
                          push_content_mock):
        #actual call
        input_urls = [
            "http://foo.com/sitemap.xml",
            "http://bar.com/sitemap.xml"
        ]
        s3_uri = "s3://foo"
        download_sitemap_files(input_urls, s3_uri, "user_agent")

        #verifications
        metadata = Metadata()
        expected_download_calls = [
            mock.call('http://foo.com/sitemap.xml', 's3://foo', 'user_agent', metadata, mock.ANY, False),
            mock.call('http://bar.com/sitemap.xml', 's3://foo', 'user_agent', metadata, mock.ANY, False)
        ]
        self.assertEqual(expected_download_calls, download_sitemap_file_mock.mock_calls)
        push_content_mock.assert_called_once_with(
            "s3://foo/sitemaps/sitemap_download_metadata.json",
            mock.ANY
        )


class TestDownloadSitemapFile(unittest.TestCase):
    @mock.patch('cdf.utils.s3.push_file', autospec=True)
    @mock.patch('cdf.features.sitemaps.tasks.download_sitemaps', autospec=True)
    def test_nominal_case(self,
                          download_sitemaps_mock,
                          push_file_mock):

        #mocking
        def f(input_url, output_directory, user_agent, metadata):
            metadata.add_success_sitemap(
                SitemapMetadata("http://foo.com/sitemap.xml",
                                SiteMapType.SITEMAP_XML,
                                "/tmp/foo/sitemap.xml")
            )
        download_sitemaps_mock.side_effect = f

        #actual call
        input_url = "http://foo.com/sitemap.xml"
        s3_uri = "s3://foo"
        metadata = Metadata()
        download_sitemap_file(input_url, s3_uri, "user_agent", metadata)

        #verifications
        expected_metadata = Metadata([
            SitemapMetadata("http://foo.com/sitemap.xml",
                            SiteMapType.SITEMAP_XML,
                            "s3://foo/sitemaps/sitemap.xml")
        ])
        self.assertEqual(expected_metadata, metadata)

        push_file_mock.assert_called_once_with(
            "s3://foo/sitemaps/sitemap.xml",
            "/tmp/foo/sitemap.xml"
        )

class TestMatchSitemapUrls(unittest.TestCase):

    @mock_s3
    def test_nominal_case(self):

        bucket = "app.foo.com"
        s3_uri = "s3://{}/crawl_result".format(bucket)
        conn = boto.connect_s3()
        bucket = conn.create_bucket(bucket)
        k1 = boto.s3.key.Key(bucket)
        k1.key = "crawl_result/sitemaps/sitemap_1.txt"
        k1.set_contents_from_string(("http://foo.com/qux\n"
                                     "http://foo.com/bar"))

        k2 = boto.s3.key.Key(bucket)
        k2.key = "crawl_result/sitemaps/sitemap_2.txt"
        k2.set_contents_from_string(("http://foo.com/index.html\n"  # not in crawl
                                     "http://bar.com"))

        k3 = boto.s3.key.Key(bucket)
        k3.key = "crawl_result/sitemaps/sitemap_download_metadata.json"
        k3.set_contents_from_string((
            '{'
            '    "sitemaps": ['
            '        {'
            '            "url": "http://foo.com/sitemap_1.txt",'
            '            "file_type": "SITEMAP_TEXT",'
            '            "sitemap_indexes": ["http://foo.com/sitemap_index.xml"],'
            '            "s3_uri": "s3://app.foo.com/crawl_result/sitemaps/sitemap_1.txt"'
            '        },'
            '        {'
            '            "url": "http://foo.com/sitemap_2.txt",'
            '            "file_type": "SITEMAP_TEXT",'
            '            "sitemap_indexes": ["http://foo.com/sitemap_index.xml"],'
            '            "s3_uri": "s3://app.foo.com/crawl_result/sitemaps/sitemap_2.txt"'
            '        }'
            '    ], '
            '    "sitemap_indexes": ['
            '        {'
            '            "url": "http://foo.com/sitemap_index.xml",'
            '            "valid_urls": 2,'
            '            "invalid_urls": 0'
            '        }'
            '    ], '
            '    "errors": []'
            '}'
        ))
        #mock definition
        ids = [
            (1, "http", "foo.com", "/bar", ""),
            (2, "http", "foo.com", "/baz", ""),
            (3, "http", "foo.com", "/qux", ""),
        ]
        IdStreamDef.persist(
            iter(ids),
            s3_uri
        )

        #call
        allowed_domains = ["foo.com"]
        blacklisted_domains = []
        first_part_id_size = 10
        part_id_size = 100

        match_sitemap_urls(s3_uri,
                           allowed_domains,
                           blacklisted_domains,
                           first_part_id_size,
                           part_id_size)

        #check output files
        self.assertEqual(['http://foo.com/index.html\n'],
                         list(stream_files("s3://app.foo.com/crawl_result/sitemap_only.gz")))

        self.assertEqual(['http://bar.com\n'],
                         list(stream_files("s3://app.foo.com/crawl_result/in_sitemap_out_of_crawl_domain.gz")))

        self.assertEqual(['1\n', '3\n'],
                         list(stream_files("s3://app.foo.com/crawl_result/sitemap.txt.0.gz")))

        #s3 uris are not there because the file will be exposed publicly
        #so we remove the technical/internal fields.
        expected_sitemap_metadata = {
            "sitemaps": [
                {
                    "url": "http://foo.com/sitemap_1.txt",
                    "file_type": "SITEMAP_TEXT",
                    "sitemap_indexes": ["http://foo.com/sitemap_index.xml"],
                    "valid_urls": 2,
                    "invalid_urls": 0
                },
                {
                    "url": "http://foo.com/sitemap_2.txt",
                    "file_type": "SITEMAP_TEXT",
                    "sitemap_indexes": ["http://foo.com/sitemap_index.xml"],
                    "valid_urls": 2,
                    "invalid_urls": 0
                }
            ],
            "sitemap_indexes": [
                {
                    "url": "http://foo.com/sitemap_index.xml",
                    "valid_urls": 2,
                    "invalid_urls": 0
                }
            ],
            "errors": [],
            "sitemap_only":
            {
                "in_configuration": 1,
                "out_of_configuration": 1
            }
        }
        key = bucket.get_key("crawl_result/sitemap_metadata.json")
        actual_sitemap_metada = json.loads(key.get_contents_as_string())
        self.assertEqual(expected_sitemap_metadata, actual_sitemap_metada)


class TestUpdateMetadata(unittest.TestCase):
    def setUp(self):
        self.download_status = Metadata()
        self.download_status.add_success_sitemap(
            SitemapMetadata("http://foo.com/sitemap_1.txt",
                            SiteMapType.SITEMAP_XML,
                            "s3://foo.com/sitemap_1.txt")
            )
        self.download_status.add_success_sitemap(
            SitemapMetadata("http://foo.com/sitemap_2.txt",
                            SiteMapType.SITEMAP_XML,
                            "s3://foo.com/sitemap_2.txt")
            )

    def test_nominal_case(self):
        document = SitemapXmlDocument("/tmp/sitemap_2.txt", "http://foo.com/sitemap_2.txt")
        document.valid_urls = 2
        document.invalid_urls = 1
        update_download_status(self.download_status, [document])

        modified_sitemap = self.download_status.sitemaps[1]
        self.assertEqual("http://foo.com/sitemap_2.txt", modified_sitemap.url)
        self.assertEqual(2, modified_sitemap.valid_urls)
        self.assertEqual(1, modified_sitemap.invalid_urls)
        self.assertIsNone(modified_sitemap.error_type)
        self.assertIsNone(modified_sitemap.error_message)

    def test_error_case(self):
        document = SitemapXmlDocument("/tmp/sitemap_2.txt", "http://foo.com/sitemap_2.txt")
        document.valid_urls = 2
        document.invalid_urls = 1
        document.error = "ParsingError"
        document.error_message = "foo"
        update_download_status(self.download_status, [document])

        modified_sitemap = self.download_status.sitemaps[1]
        self.assertEqual("http://foo.com/sitemap_2.txt", modified_sitemap.url)
        self.assertEqual(2, modified_sitemap.valid_urls)
        self.assertEqual(1, modified_sitemap.invalid_urls)
        self.assertEqual("ParsingError", modified_sitemap.error_type)
        self.assertEqual("foo", modified_sitemap.error_message)


class TestSaveUrlListAsGzip(unittest.TestCase):
    def setUp(self):
        self.filename = "output_file.gz"
        self.tmp_dir = "/tmp/azerty"
        self.output_file = "/tmp/azerty/output_file.gz"

    def test_nominal_case(self):
        url_list = ["foo", "bar"]
        with mock.patch("cdf.features.sitemaps.tasks.gzip.open", mock.mock_open()) as m:
            actual_result = save_url_list_as_gzip(url_list,
                                                  self.filename,
                                                  self.tmp_dir)
        self.assertEqual(actual_result, self.output_file)
        expected_calls = [mock.call("foo\n"), mock.call("bar\n")]
        self.assertEqual(expected_calls, m().write.mock_calls)

    def test_unicode_case(self):
        url_list = [u"föö"]
        with mock.patch("cdf.features.sitemaps.tasks.gzip.open", mock.mock_open()) as m:
            actual_result = save_url_list_as_gzip(url_list,
                                                  self.filename,
                                                  self.tmp_dir)
        self.assertEqual(actual_result, self.output_file)
        expected_calls = [mock.call(u"föö\n".encode("utf-8"))]
        self.assertEqual(expected_calls, m().write.mock_calls)
