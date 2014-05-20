import tempfile
import os
import gzip
import unittest
import shutil

from mock import patch

from cdf.features.main.streams import IdStreamDef
from cdf.features.ganalytics.streams import VisitsStreamDef
from cdf.features.ganalytics.tasks import match_analytics_to_crawl_urls
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
        f.write('www.site.com/5?sid=5\torganic\tgoogle\t40\n')
        f.write('www.site.com/1\torganic\tgoogle\t5\n')
        f.write('www.site.com/3\torganic\tgoogle\t10\n')
        f.write('www.site.com/2\torganic\tgoogle\t3\n')
        f.write('www.site.com/4\torganic\tgoogle\t12\n')
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
            list(VisitsStreamDef.get_stream_from_s3(self.s3_dir, tmp_dir=self.tmp_dir)),
            [
                [1, "organic", "google", 5],
                [2, "organic", "google", 3],
                [3, "organic", "google", 10],
                [4, "organic", "google", 12],
                [5, "organic", "google", 40],
            ]
        )
