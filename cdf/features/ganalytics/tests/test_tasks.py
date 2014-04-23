import tempfile
import os
import gzip
import unittest

from mock import patch

from cdf.features.main.streams import IdStreamDef
from cdf.features.ganalytics.tasks import transform_visits_to_local_ids
from cdf.core.mocks import _mock_push_file, _mock_push_content, _mock_fetch_file, _mock_fetch_files


class TestTasks(unittest.TestCase):

    @patch('cdf.utils.s3.push_file', _mock_push_file)
    @patch('cdf.utils.s3.push_content', _mock_push_content)
    @patch('cdf.utils.s3.fetch_file', _mock_fetch_file)
    @patch('cdf.utils.s3.fetch_files', _mock_fetch_files)
    def test_transform_visits_to_local_ids(self):
        tmp_dir = tempfile.mkdtemp()
        crawl_data_dir = 's3://' + tempfile.mkdtemp()
        raw_visits_location = os.path.join(tmp_dir, 'raw_visits.txt.gz')
        f = gzip.open(raw_visits_location, 'w')
        f.write('//www.site.com/1\torganic\tgoogle\t5\n')
        f.write('//www.site.com/2\torganic\tgoogle\t3\n')
        f.write('//www.site.com/3\torganic\tgoogle\t10\n')
        f.write('//www.site.com/4\torganic\tgoogle\t12\n')
        f.write('//www.site.com/5?sid=5\torganic\tgoogle\t40\n')
        f.close()

        f = IdStreamDef.create_temporary_dataset()
        f.append(1, "http", "www.site.com", "/1")
        f.append(2, "http", "www.site.com", "/2")
        f.append(3, "http", "www.site.com", "/3")
        f.append(4, "http", "www.site.com", "/4")
        f.append(5, "http", "www.site.com", "/5")
        f.persist_to_storage(crawl_data_dir, part_id=0)

        transform_visits_to_local_ids(raw_visits_location, crawl_data_dir, first_part_id_size=3, part_id_size=2, tmp_dir=tmp_dir)

        import pdb; pdb.set_trace()
