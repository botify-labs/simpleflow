import unittest
import tempfile
import shutil
from moto import mock_s3
import boto

from cdf.features.links.streams import (
    OutlinksStreamDef,
    BadLinksStreamDef,
    BadLinksCountersStreamDef,
    OutlinksCountersStreamDef,
    OutcanonicalCountersStreamDef,
    OutredirectCountersStreamDef
)
from cdf.features.links.tasks import (
    make_bad_link_file as compute_bad_link,
    make_links_counter_file as compute_link_counter,
    make_bad_link_counter_file as compute_bad_link_counter
)
from cdf.features.main.streams import InfosStreamDef
from cdf.utils.s3 import list_files


# TODO(darkjh) remove duplication with stream-level test
class TestBadLinksTask(unittest.TestCase):
    def setUp(self):
        self.info = [
            [1, 0, '', 1, 12345, 200, 1, 1, 1],
            [2, 0, '', 1, 12345, 301, 1, 1, 1],
            [3, 0, '', 1, 12345, 500, 1, 1, 1],
        ]
        self.outlinks = [
            [4, 'a', 0, 1],
            [4, 'a', 0, 2],
            [5, 'a', 0, 1],
            [5, 'a', 0, 3],
            [6, 'canonical', 0, 2]
        ]
        self.tmp_dir = tempfile.mkdtemp()
        self.first_part_size = 2
        self.part_size = 1

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    @mock_s3
    def test_harness(self):
        s3_uri = 's3://test_bucket'
        s3 = boto.connect_s3()
        s3.create_bucket('test_bucket')

        InfosStreamDef.persist(
            iter(self.info),
            s3_uri,
            first_part_size=self.first_part_size,
            part_size=self.part_size
        )

        OutlinksStreamDef.persist(
            iter(self.outlinks),
            s3_uri,
            first_part_size=self.first_part_size,
            part_size=self.part_size
        )

        compute_bad_link(
            1234, s3_uri,
            first_part_id_size=self.first_part_size,
            part_id_size=self.part_size
        )
        self.assertEqual(len(list_files(s3_uri, '.*badlinks.*')), 2)

        result = list(BadLinksStreamDef.load(s3_uri, tmp_dir=self.tmp_dir))
        expected = [
            [4, 2, 301],
            [5, 3, 500]
        ]
        self.assertEqual(result, expected)


class TestLinksCounterTask(unittest.TestCase):
    def setUp(self):
        self.outlinks = [
            [1, 'a', 0, 2, ''],
            [1, 'a', 1, 3, ''],
            [1, 'a', 0, 4, ''],
            [3, 'canonical', 5, 5, ''],
            [4, 'r301', 5, 5, ''],
        ]
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    @mock_s3
    def test_outgoing_harness(self):
        s3 = boto.connect_s3()
        s3.create_bucket('test_bucket')
        s3_uri = 's3://test_bucket'

        stream_args = {
            'uri': s3_uri,
            'tmp_dir': self.tmp_dir,
            'part_id': 0
        }

        OutlinksStreamDef.persist(
            iter(self.outlinks), s3_uri
        )

        compute_link_counter(1234, s3_uri, part_id=0, link_direction='out')

        # check links
        expected = [
            [1, ['follow'], True, 2, 2], [1, ['link'], True, 1, 1]
        ]
        result = list(OutlinksCountersStreamDef.load(**stream_args))
        self.assertItemsEqual(expected, result)

        # check canonical
        expected = [[3, False]]
        result = list(OutcanonicalCountersStreamDef.load(**stream_args))
        self.assertItemsEqual(expected, result)

        # check redirection
        expected = [[4, True]]
        result = list(OutredirectCountersStreamDef.load(**stream_args))
        self.assertItemsEqual(expected, result)


class TestBadLinkCounterTask(unittest.TestCase):
    def setUp(self):
        self.badlinks = [
            [1, 2, 500],
            [1, 9, 500],
            [1, 2, 400],
        ]
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    @mock_s3
    def test_harness(self):
        s3 = boto.connect_s3()
        s3.create_bucket('test_bucket')
        s3_uri = 's3://test_bucket'
        part_id = 64

        BadLinksStreamDef.persist(
            iter(self.badlinks),
            s3_uri,
            part_id=part_id
        )

        compute_bad_link_counter(
            1234, s3_uri,
            part_id=part_id
        )

        result = list(BadLinksCountersStreamDef.load(
            s3_uri,
            tmp_dir=self.tmp_dir,
            part_id=part_id
        ))
        expected = [
            [1, 400, 1],  # 1 400 link
            [1, 500, 2],  # 2 500 link
        ]
        self.assertEqual(result, expected)