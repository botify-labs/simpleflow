import shutil
import tempfile
import unittest

from cdf.features.main.streams import (
    CompliantUrlStreamDef,
    IdStreamDef, InfosStreamDef
)
from cdf.features.rel.streams import RelStreamDef, RelCompliantStreamDef
from cdf.features.rel.tasks import convert_rel_out_to_rel_compliant_out
from cdf.features.rel import constants as rel_constants

import boto
from moto import mock_s3

from bitarray import bitarray


def _next_doc(generator):
    return next(generator)[1]


class TestTasks(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.s3_uri = 's3://test_bucket/analysis'
        self.first_part_size = 10
        self.part_size = 100

        self.compliant = [
            [1, 'true', 0],
            [2, 'true', 0],
            [3, 'true', 0],
            [4, 'false', 1],
        ]

        # Rel stream format
        # uid_from type mask uid_to url value
        # type :
        # 1 = hreflang
        # 2 = prev
        # 3 = next
        # 4 = author
        self.rel = [
            [1, 1, 0, 2, "", "en-US"], # OK
            [1, 1, 0, -1, "http://www.site.com/it", "it-IT"], # OK but warning to external URL
            [1, 1, 0, 3, "", "jj-us"], # KO : Bad Lang
            [1, 1, 0, 4, "", "en-ZZ"], # KO : Bad Country
        ]
        self.expected_compliant = [
            True,
            None,
            True,
            False,
        ]

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    @mock_s3
    def test_convert_rel_out_to_rel_compliant_out(self):
        conn = boto.connect_s3()
        bucket = conn.create_bucket('test_bucket')

        # Persist rel
        rs = RelStreamDef.create_temporary_dataset()
        for r in self.rel:
            rs.append(*r)
        rs.persist(self.s3_uri, first_part_size=self.first_part_size, part_size=self.part_size)

        # Persist compliant urls
        cu = CompliantUrlStreamDef.create_temporary_dataset()
        for c in self.compliant:
            cu.append(*c)
        cu.persist(self.s3_uri, first_part_size=self.first_part_size, part_size=self.part_size)

        convert_rel_out_to_rel_compliant_out(
            1, self.s3_uri, self.tmp_dir,
            first_part_id_size=self.first_part_size, part_id_size=self.part_size, crawled_partitions=[0])

        self.assertEquals(
            list(RelCompliantStreamDef.load(self.s3_uri, tmp_dir=self.tmp_dir)),
            map(list.__add__, self.rel, [[k] for k in self.expected_compliant])
        )
