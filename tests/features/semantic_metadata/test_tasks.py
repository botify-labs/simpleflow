import unittest
from moto import mock_s3
import boto

import os
import tempfile
import shutil

from cdf.analysis.urls.transducers.metadata_duplicate import notset_hash_value
from cdf.features.semantic_metadata.streams import (
    ContentsCountStreamDef,
    ContentsStreamDef
)
from cdf.features.semantic_metadata.tasks import compute_metadata_count


class TestComputeMetadataCount(unittest.TestCase):
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
        fake_hash = 1597530492

        contents = [
            (1, 1, fake_hash, "foo title"),
            (1, 2, fake_hash, "foo description"),
            (1, 3, fake_hash, "foo h1"),
            (1, 1, fake_hash, "bar title"),
            (1, 3, fake_hash, "bar h1"),
            (1, 4, notset_hash_value, "not set h2"),
            (2, 1, fake_hash, "foo title 2")
        ]
        ContentsStreamDef.persist_part_to_s3(iter(contents),
                                             self.s3_uri,
                                             part_id=part_id)

        #actual call
        file_uri = compute_metadata_count(self.s3_uri, part_id)

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

