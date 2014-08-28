import unittest
from moto import mock_s3
import boto
import tempfile
import shutil
from cdf.features.main.streams import (
    IdStreamDef,
    ZoneStreamDef,
    InfosStreamDef
)
from cdf.features.main.tasks import compute_zones


class TestComputeZones(unittest.TestCase):
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

        #create urlids
        urlids = boto.s3.key.Key(bucket)
        urlids.key = "crawl_result/urlids.txt.0.gz"
        ids = iter([
            (1, "http", "foo.com", "/"),
            (2, "https", "foo.com", "/bar"),
            (9, "https", "foo.com", "/baz")
        ])
        IdStreamDef.persist_part_to_s3(ids, self.s3_uri, part_id=0)

        #create urlinfos
        contents = iter([
            (1, 0, "text/html", 0, 0, 0, 0, 0, 0, "en-US"),
            (2, 0, "text/html", 0, 0, 0, 0, 0, 0, "fr"),
            (9, 0, "text/html", 0, 0, 0, 0, 0, 0, "fr")
        ])
        InfosStreamDef.persist_part_to_s3(contents, self.s3_uri, part_id=0)

        #actual computation
        part_id = 0
        document_uri = compute_zones(self.s3_uri,
                                     part_id)

        #check output
        expected_document_uri = "{}/zones.txt.{}.gz".format(self.s3_uri, part_id)
        self.assertEqual(expected_document_uri, document_uri)

        zone_stream = ZoneStreamDef.load(
            self.s3_uri,
            tmp_dir=self.tmp_dir,
            part_id=part_id
        )
        expected_zone_stream = [
            [1, 'en-US,http'],
            [2, 'fr,https'],
            [9, 'fr,https']
        ]
        self.assertEqual(expected_zone_stream, list(zone_stream))

    @mock_s3
    def test_unexisting_part(self):

        conn = boto.connect_s3()
        conn.create_bucket(self.bucket_name)

        #actual computation
        part_id = 0
        document_uri = compute_zones(self.s3_uri,
                                     part_id)

        #check output
        expected_document_uri = "{}/zones.txt.{}.gz".format(self.s3_uri, part_id)
        self.assertEqual(expected_document_uri, document_uri)

        zone_stream = ZoneStreamDef.load(
            self.s3_uri,
            tmp_dir=self.tmp_dir,
            part_id=part_id
        )
        self.assertEqual([], list(zone_stream))
