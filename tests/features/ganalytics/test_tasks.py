import tempfile
import os
import gzip
import unittest
import shutil
import datetime

from mock import patch

from cdf.features.main.streams import IdStreamDef, InfosStreamDef
from cdf.features.ganalytics.streams import VisitsStreamDef
from cdf.features.ganalytics.tasks import (import_data_from_ganalytics,
                                           get_api_requests,
                                           match_analytics_to_crawl_urls)
from cdf.core.mocks import _mock_push_file, _mock_push_content, _mock_fetch_file, _mock_fetch_files


class TestImportDataFromGanalytics(unittest.TestCase):
    @patch("cdf.features.ganalytics.tasks.load_analytics_metadata")
    @patch("cdf.features.ganalytics.tasks.get_credentials")
    @patch("cdf.features.ganalytics.tasks.import_data")
    @patch('cdf.utils.s3.push_file')
    def test_date_start_date_end_default_values(self, mock_push, mock_import,
                                                mock_credentials,
                                                mock_load_metadata):

        mock_credentials.return_value = "mock_credentials"
        mock_load_metadata.return_value = {
            "sample_rate": 1.0,
            "sample_size": 100,
            "sampled": False,
            "queries_count": 10
        }

        access_token = "access_token"
        refresh_token = "refresh_token"
        ganalytics_site_id = "12345678"
        s3_uri = "s3_uri"
        tmp_dir = "/tmp/mock"
        force_fetch = False
        import_data_from_ganalytics(access_token,
                                    refresh_token,
                                    ganalytics_site_id,
                                    s3_uri,
                                    date_start=None,
                                    date_end=None,
                                    tmp_dir=tmp_dir,
                                    force_fetch=force_fetch)

        expected_start_date = datetime.date.today() - datetime.timedelta(31)
        expected_end_date = datetime.date.today() - datetime.timedelta(1)

        mock_import.assert_called_once_with("ga:12345678",
                                            'mock_credentials',
                                            expected_start_date,
                                            expected_end_date,
                                            tmp_dir)


class TestGetApiRequests(unittest.TestCase):
    def test_nominal_case(self):
        analytics_metadata = {
            "sample_rate": 1.0,
            "sample_size": 100,
            "sampled": False,
            "queries_count": 10
        }

        ghost_pages_session_count = {
            'organic.all': 100,
            'organic.google': 80,
            'organic.bing': 5,
            'social.all': 100,
            'social.facebook': 70,
            'social.twitter': 20
        }

        actual_result = get_api_requests(analytics_metadata,
                                         ghost_pages_session_count)
        expected_result = {
            "api_requests": [
                {
                    "method": "patch",
                    "endpoint_url": "revision",
                    "endpoint_suffix": "ganalytics/",
                    "data": {
                        "sample_rate": 1.0,
                        "sample_size": 100,
                        "sampled": False,
                        "queries_count": 10,
                        "ghost": {
                            "organic": {
                                "all": 100,
                                "google": 80,
                                "bing": 5
                            },
                            "social": {
                                "all": 100,
                                "facebook": 70,
                                "twitter": 20
                            }
                        }

                    }
                }
            ]
        }
        self.assertEqual(expected_result, actual_result)


class TestTasks(unittest.TestCase):

    def setUp(self):
        self.first_part_id_size = 3
        self.part_id_size = 2
        self.tmp_dir = tempfile.mkdtemp()
        self.s3_dir = "s3://" + tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        shutil.rmtree(self.s3_dir[5:])

    @patch("cdf.features.ganalytics.streams.ORGANIC_SOURCES", ["google"])
    @patch("cdf.features.ganalytics.streams.SOCIAL_SOURCES", ["facebook"])
    @patch('cdf.utils.s3.push_file', _mock_push_file)
    @patch('cdf.utils.s3.push_content', _mock_push_content)
    @patch('cdf.utils.s3.fetch_file', _mock_fetch_file)
    @patch('cdf.utils.s3.fetch_files', _mock_fetch_files)
    def test_match_analytics_to_crawl_urls(self):
        raw_visits_location = os.path.join(self.s3_dir[5:], 'analytics.data.gz')
        f = gzip.open(raw_visits_location, 'w')
        f.write('www.site.com/5?sid=5\torganic\tgoogle\t(not set)\t30\t25\t32\t100\t16\t25\n')
        f.write('www.site.com/1\torganic\tgoogle\t(not set)\t5\t3\t4\t26\t3\t1\n')
        f.write('www.site.com/3\torganic\tgoogle\t(not set)\t8\t1\t8\t5\t5\t5\n')
        f.write('www.site.com/2\torganic\tgoogle\t(not set)\t3\t3\t2\t10\t1\t0\n')
        f.write('www.site.com/4\torganic\tgoogle\t(not set)\t11\t4\t15\t54\t8\t8\n')
        f.write('www.site.com/5\torganic\tgoogle\t(not set)\t7\t3\t1\t23\t7\t9\n')  # ghost page
        f.close()

        f = IdStreamDef.create_temporary_dataset()
        f.append(1, "http", "www.site.com", "/1", "")
        f.append(2, "http", "www.site.com", "/2", "")
        f.append(3, "http", "www.site.com", "/3", "")
        f.append(4, "http", "www.site.com", "/4", "")
        f.append(5, "http", "www.site.com", "/5", "?sid=5")
        f.append(6, "http", "www.site.com", "/6", "")
        f.append(7, "https", "www.site.com", "/4", "")  # ambiguous url (http version exists)
        f.persist_to_s3(self.s3_dir, first_part_id_size=self.first_part_id_size, part_id_size=self.part_id_size)
        ('id', int),
        ('infos_mask', int),
        ('content_type', str),
        ('depth', int),
        ('date_crawled', int),
        ('http_code', int),
        ('byte_size', int),
        ('delay_first_byte', int),
        ('delay_last_byte', int),

        f = InfosStreamDef.create_temporary_dataset()
        f.append(1, 0, "", 0, 0, 200, 0, 0, 0)
        f.append(2, 0, "", 0, 0, 200, 0, 0, 0)
        f.append(3, 0, "", 0, 0, 200, 0, 0, 0)
        f.append(4, 0, "", 0, 0, 200, 0, 0, 0)
        f.append(5, 0, "", 0, 0, 200, 0, 0, 0)
        f.append(6, 0, "", 0, 0, 200, 0, 0, 0)
        f.append(7, 0, "", 0, 0, 200, 0, 0, 0)  # ambiguous url has code 200
        f.persist_to_s3(self.s3_dir, first_part_id_size=self.first_part_id_size, part_id_size=self.part_id_size)

        #fake analytics metadata
        analytics_metadata_location = os.path.join(self.s3_dir[5:], 'analytics.meta.json')
        f = open(analytics_metadata_location, "w")
        f.write('{"sample_rate": 1.0, "sample_size": 100, "sampled": false, "queries_count": 10}')
        f.close()

        actual_result = match_analytics_to_crawl_urls(
            self.s3_dir,
            first_part_id_size=self.first_part_id_size,
            part_id_size=self.part_id_size,
            tmp_dir=self.tmp_dir)

        self.assertEquals(
            #
            list(VisitsStreamDef.get_stream_from_s3(self.s3_dir, tmp_dir=self.tmp_dir)),
            [
                [1, "organic", "google", 'None', 5, 3, 4, 26, 3, 1],
                [2, "organic", "google", 'None', 3, 3, 2, 10, 1, 0],
                [3, "organic", "google", 'None', 8, 1, 8, 5, 5, 5],
                [4, "organic", "google", 'None', 11, 4, 15, 54, 8, 8],
                [5, "organic", "google", 'None', 30, 25, 32, 100, 16, 25],
            ]
        )

        #check ambiguous visits
        with gzip.open(os.path.join(self.s3_dir[5:], 'ambiguous_urls_dataset.gz')) as f:
            expected_result = ['www.site.com/4\torganic\tgoogle\tNone\t11\t4\t15\t54.0\t8\t8\n']
            self.assertEquals(expected_result, f.readlines())

        expected_result = {
            "api_requests": [
                {
                    "method": "patch",
                    "endpoint_url": "revision",
                    "endpoint_suffix": "ganalytics/",
                    "data": {
                        "sample_rate": 1.0,
                        "sample_size": 100,
                        "sampled": False,
                        "queries_count": 10,
                        "ghost": {
                            "organic": {
                                "all": {
                                    "nb_urls": 1,
                                    "nb_visits": 7
                                },
                                "google": {
                                    "nb_urls": 1,
                                    "nb_visits": 7
                                }
                            },
                            "social": {
                                "all": {
                                    "nb_urls": 0,
                                    "nb_visits": 0
                                },
                                "facebook": {
                                    "nb_urls": 0,
                                    "nb_visits": 0
                                }
                            }
                        }
                    }
                }
            ]
        }
        self.assertEqual(expected_result, actual_result)


