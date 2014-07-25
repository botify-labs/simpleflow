import unittest
import mock

import os
import tempfile
from cdf.core.streams.base import TemporaryDataset
from cdf.features.sitemap.document import (SitemapXmlDocument,
                                           SitemapTextDocument,
                                           SiteMapType)
from cdf.features.sitemap.download import (SitemapMetadata,
                                           SitemapIndexMetadata,
                                           Error,
                                           DownloadStatus)
from cdf.features.sitemap.matching import (get_download_status_from_s3,
                                           download_sitemaps_from_s3,
                                           match_sitemap_urls_from_document,
                                           match_sitemap_urls_from_documents,
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
                        '"sitemap_indexes": ['
                        '    {'
                        '        "url": "http://foo/sitemap_index.xml",'
                        '        "valid_urls": 2,'
                        '        "invalid_urls": 0'
                        '    }'
                        '],'
                        '"errors": ['
                        '    {'
                        '        "url": "http://error",'
                        '        "type": "UNKNOWN",'
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
            SitemapMetadata("http://foo/sitemap_1.xml",
                            "s3://foo/sitemap_1.xml",
                            "http://foo/sitemap_index.html"),
            SitemapMetadata("http://foo/sitemap_2.xml",
                            "s3://foo/sitemap_2.xml",
                            "http://foo/sitemap_index.html"),
        ]
        expected_sitemap_indexes = [SitemapIndexMetadata("http://foo/sitemap_index.xml", 2, 0)]
        expected_errors = [Error(u"http://error", SiteMapType.UNKNOWN, u"DownloadError", u"foo")]
        expected_result = DownloadStatus(expected_sitemaps,
                                         expected_sitemap_indexes,
                                         expected_errors)
        self.assertEqual(expected_result, actual_result)

        #check calls
        m.assert_called_once_with("/tmp/foo/sitemap_download_metadata.json")
        fetch_file_mock.assert_called_once_with("s3://foo/sitemaps/sitemap_download_metadata.json",
                                                "/tmp/foo/sitemap_download_metadata.json",
                                                force_fetch)


class TestDownloadSitemapsFromS3(unittest.TestCase):
    @mock.patch('cdf.features.sitemap.matching.get_download_status_from_s3', autospec=True)
    @mock.patch('cdf.utils.s3.fetch_file', autospec=True)
    def test_nominal_case(self,
                          fetch_file_mock,
                          get_download_status_from_s3_mock):
        #mock
        sitemaps = [
            SitemapMetadata("http://foo.com/sitemap_1.xml", "s3://foo/sitemap_1.xml"),
            SitemapMetadata("http://foo.com/sitemap_2.xml", "s3://foo/sitemap_2.xml")
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


class MatchSitemapUrlsFromDocument(unittest.TestCase):
    def test_nominal_case(self):

        url_to_id = {
            "foo": 0,
            "bar": 2,
            "qux": 5
        }
        document_mock = mock.create_autospec(SitemapXmlDocument)
        document_mock.get_urls.return_value = iter(["foo", "bar", "baz", "qux", "donald", "mickey"])

        dataset = mock.create_autospec(TemporaryDataset)
        dataset = mock.MagicMock()

        domain_validator = mock.create_autospec(DomainValidator)
        domain_validator.is_valid.side_effect = [True, False, False]

        sitemap_only_nb_samples = 2  # one url will be skipped
        sitemap_only_urls = []
        out_of_crawl_domain_urls = []
        match_sitemap_urls_from_document(document_mock,
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

        document_mock = mock.create_autospec(SitemapXmlDocument)
        document_mock.get_urls.return_value = iter(["foo", "bar", "foo", "baz", "qux"])

        dataset = mock.create_autospec(TemporaryDataset)
        dataset = mock.MagicMock()

        domain_validator = mock.create_autospec(DomainValidator)
        domain_validator.is_valid.return_value = True

        sitemap_only_nb_samples = 3  # one url will be skipped
        sitemap_only_urls = []
        out_of_crawl_domain_urls = []
        match_sitemap_urls_from_document(document_mock,
                                         url_to_id,
                                         dataset,
                                         domain_validator,
                                         sitemap_only_nb_samples,
                                         sitemap_only_urls,
                                         out_of_crawl_domain_urls)

        self.assertEqual(["foo", "bar", "baz"], sitemap_only_urls)
        self.assertEqual([], out_of_crawl_domain_urls)


class TestMatchSitemapUrlsFromDocuments(unittest.TestCase):

    def test_parsing_error_case(self):
        file1 = tempfile.NamedTemporaryFile(delete=False)
        file1.write('<?xml version="1.0" encoding="UTF-8"?>\n'
                    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
                    '<url><loc>http://foo/bar</loc></url>\n'
                    '><\n'  # syntax error
                    '<url><loc>http://foo/baz</loc></url>\n'
                    '</urlset>\n')
        file1.close()
        document_1 = SitemapXmlDocument(file1.name, "http://foo.com/sitemap_1.xml")

        file2 = tempfile.NamedTemporaryFile(delete=False)
        file2.write(("http://foo.com/index.html\n"
                     "http://bar.com"))
        file2.close()
        document_2 = SitemapTextDocument(file2.name, "http://foo.com/sitemap_2.txt")

        documents = [document_1, document_2]

        url_to_id = {}

        dataset = mock.create_autospec(TemporaryDataset)
        dataset = mock.MagicMock()

        domain_validator = mock.create_autospec(DomainValidator)
        domain_validator.is_valid.return_value = True

        sitemap_only_nb_samples = 10
        sitemap_only_urls = []
        out_of_crawl_domain_urls = []

        #call
        match_sitemap_urls_from_documents(documents,
                                          url_to_id,
                                          dataset,
                                          domain_validator,
                                          sitemap_only_nb_samples,
                                          sitemap_only_urls,
                                          out_of_crawl_domain_urls)

        #check that error message is present
        #do not check document valid_urls, invalid_urls count
        #this is the role of test_parsing_error_case_count_urls
        #(currently skipped because of libxml2 version issue)
        self.assertEqual("ParsingError", document_1.error)
        self.assertEqual("StartTag: invalid element name, line 4, column 3",
                         document_1.error_message)

        os.remove(file1.name)
        os.remove(file2.name)

    @unittest.skip("Requires libxml2 2.9.1 (available in Ubuntu 14.04)")
    def test_parsing_error_case_count_urls(self):
        file1 = tempfile.NamedTemporaryFile(delete=False)
        file1.write('<?xml version="1.0" encoding="UTF-8"?>\n'
                    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
                    '<url><loc>http://foo/bar</loc></url>\n'
                    '><\n'  # syntax error
                    '<url><loc>http://foo/baz</loc></url>\n'
                    '</urlset>\n')
        file1.close()
        document_1 = SitemapXmlDocument(file1.name, "http://foo.com/sitemap_1.xml")

        file2 = tempfile.NamedTemporaryFile(delete=False)
        file2.write(("http://foo.com/index.html\n"
                     "http://bar.com"))
        file2.close()
        document_2 = SitemapTextDocument(file2.name, "http://foo.com/sitemap_2.txt")

        documents = [document_1, document_2]

        url_to_id = {}

        dataset = mock.create_autospec(TemporaryDataset)
        dataset = mock.MagicMock()

        domain_validator = mock.create_autospec(DomainValidator)
        domain_validator.is_valid.return_value = True

        sitemap_only_nb_samples = 10
        sitemap_only_urls = []
        out_of_crawl_domain_urls = []

        #call
        match_sitemap_urls_from_documents(documents,
                                          url_to_id,
                                          dataset,
                                          domain_validator,
                                          sitemap_only_nb_samples,
                                          sitemap_only_urls,
                                          out_of_crawl_domain_urls)

        #check documents
        self.assertEqual(1, document_1.valid_urls)
        self.assertEqual(0, document_1.invalid_urls)
        self.assertEqual("ParsingError", document_1.error)
        self.assertEqual("StartTag: invalid element name, line 4, column 3",
                         document_1.error_message)

        self.assertEqual(2, document_2.valid_urls)
        self.assertEqual(0, document_2.invalid_urls)
        self.assertIsNone(document_2.error)
        self.assertIsNone(document_2.error_message)

        os.remove(file1.name)
        os.remove(file2.name)


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
