import unittest
from moto import mock_s3
import boto

import os
import gzip
import tempfile
import shutil

from cdf.analysis.urls.transducers.metadata_duplicate import notset_hash_value
from cdf.features.semantic_metadata.streams import ContentsCountStreamDef
from cdf.features.semantic_metadata.tasks import compute_metadata_filled_nb


class TestComputeMetadataFilledNb(unittest.TestCase):
    def setUp(self):
        self.bucket_name = "app.foo.com"
        self.s3_uri = "s3://{}/crawl_result".format(self.bucket_name)

        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    @mock_s3
    def test_nominal_case(self):
        conn = boto.connect_s3()
        bucket = conn.create_bucket(self.bucket_name)
        part_id = 1

        #create urlcontents
        urlcontents = boto.s3.key.Key(bucket)
        urlcontents.key = "crawl_result/urlcontents.txt.{}.gz".format(part_id)
        f = tempfile.NamedTemporaryFile(delete=False, prefix=self.tmp_dir)
        f.close()
        fake_hash = 1597530492
        with gzip.open(f.name, "w") as tmp_file:
            tmp_file.write("1\t1\t{}\tfoo title\n".format(fake_hash))
            tmp_file.write("1\t2\t{}\tfoo description\n".format(fake_hash))
            tmp_file.write("1\t3\t{}\tfoo h1\n".format(fake_hash))
            tmp_file.write("1\t1\t{}\tbar title\n".format(fake_hash))
            tmp_file.write("1\t3\t{}\tbar h1\n".format(fake_hash))
            tmp_file.write("1\t4\t{}\tnot set h2\n".format(notset_hash_value))
            tmp_file.write("2\t1\t{}\tfoo title 2\n".format(fake_hash))
        urlcontents.set_contents_from_filename(f.name)

        #actual call
        file_uri = compute_metadata_filled_nb(self.s3_uri, part_id)


        #check results
        expected_file_uri = os.path.join(
            self.s3_uri,
            "{}.txt.{}.gz".format(ContentsCountStreamDef.FILE, part_id)
        )
        self.assertEqual(
            expected_file_uri,
            file_uri
        )
        expected_stream = [
            [1, 1, 2],
            [1, 2, 1],
            [1, 3, 2],
            [2, 1, 1]
        ]
        actual_stream = ContentsCountStreamDef.get_stream_from_s3(
            self.s3_uri,
            tmp_dir=self.tmp_dir,
            part_id=part_id
        )
        self.assertEqual(expected_stream, list(actual_stream))

