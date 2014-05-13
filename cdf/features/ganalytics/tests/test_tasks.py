import tempfile
import os
import gzip
import unittest
import shutil

from mock import patch

from cdf.features.main.streams import IdStreamDef
from cdf.features.ganalytics.streams import VisitsStreamDef
from cdf.features.ganalytics.tasks import (match_analytics_to_crawl_urls,
                                          get_urlid)
from cdf.core.mocks import _mock_push_file, _mock_push_content, _mock_fetch_file, _mock_fetch_files


class TestTasks(unittest.TestCase):

    def setUp(self):
        self.first_part_id_size = 3
        self.part_id_size = 2
        self.tmp_dir = tempfile.mkdtemp()
        self.s3_dir = "s3://" + tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        shutil.rmtree(self.s3_dir[5:])

    @patch('cdf.utils.s3.push_file', _mock_push_file)
    @patch('cdf.utils.s3.push_content', _mock_push_content)
    @patch('cdf.utils.s3.fetch_file', _mock_fetch_file)
    @patch('cdf.utils.s3.fetch_files', _mock_fetch_files)
    def test_match_analytics_to_crawl_urls(self):
        raw_visits_location = os.path.join(self.s3_dir[5:], 'analytics.data.gz')
        f = gzip.open(raw_visits_location, 'w')
        f.write('www.site.com/5?sid=5\torganic\tgoogle\t(not set)\t40\t30\t25\t32\t100\t16\t25\n')
        f.write('www.site.com/1\torganic\tgoogle\t(not set)\t5\t5\t3\t4\t26\t3\t1\n')
        f.write('www.site.com/3\torganic\tgoogle\t(not set)\t10\t8\t1\t8\t5\t5\t5\n')
        f.write('www.site.com/2\torganic\tgoogle\t(not set)\t3\t3\t3\t2\t10\t1\t0\n')
        f.write('www.site.com/4\torganic\tgoogle\t(not set)\t12\t11\t4\t15\t54\t8\t8\n')
        f.close()

        f = IdStreamDef.create_temporary_dataset()
        f.append(1, "http", "www.site.com", "/1", "")
        f.append(2, "http", "www.site.com", "/2", "")
        f.append(3, "http", "www.site.com", "/3", "")
        f.append(4, "http", "www.site.com", "/4", "")
        f.append(5, "http", "www.site.com", "/5", "?sid=5")
        f.append(6, "http", "www.site.com", "/6", "")
        f.persist_to_s3(self.s3_dir, first_part_id_size=self.first_part_id_size, part_id_size=self.part_id_size)

        match_analytics_to_crawl_urls(self.s3_dir,
                                      first_part_id_size=self.first_part_id_size,
                                      part_id_size=self.part_id_size,
                                      tmp_dir=self.tmp_dir)

        self.assertEquals(
            #
            list(VisitsStreamDef.get_stream_from_s3(self.s3_dir, tmp_dir=self.tmp_dir)),
            [
                [1, "organic", "google", 'None', 5, 5, 3, 4, 26, 3, 1],
                [2, "organic", "google", 'None', 3, 3, 3, 2, 10, 1, 0],
                [3, "organic", "google", 'None', 10, 8, 1, 8, 5, 5, 5],
                [4, "organic", "google", 'None', 12, 11, 4, 15, 54, 8, 8],
                [5, "organic", "google", 'None', 40, 30, 25, 32, 100, 16, 25],
            ]
        )


class TestGetUrlid(unittest.TestCase):
    def setUp(self):
        self.url_to_id = {
            "http://foo.com": 0,
            "http://foo.com/bar": 1,
            "https://foo.com/baz": 2
        }

    def test_nominal_case_http(self):
        entry = ["foo.com/bar"]
        actual_result = get_urlid(entry, self.url_to_id, "http")
        self.assertEqual(1, actual_result)

    def test_nominal_case_https(self):
        entry = ["foo.com/baz"]
        actual_result = get_urlid(entry, self.url_to_id, "https")
        self.assertEqual(2, actual_result)

    def test_ambiguity(self):
        url_to_id = {
            "http://foo.com": 0,
            "https://foo.com": 1,
        }
        entry = ["foo.com"]
        #depending on the preferred protocol, the url id changes
        self.assertEqual(0, get_urlid(entry, url_to_id, "http"))
        self.assertEqual(1, get_urlid(entry, url_to_id, "https"))

    def test_wrong_protocol(self):
        entry = ["foo.com/baz"]
        actual_result = get_urlid(entry, self.url_to_id, "http")
        #the url id is not found since its protocol is not http
        self.assertIsNone(actual_result)

    def test_unexisting_url(self):
        entry = ["bar.com"]
        actual_result = get_urlid(entry, self.url_to_id, "http")
        self.assertIsNone(actual_result)
