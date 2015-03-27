import unittest
import os
from cdf.utils.path import partition_aware_sort
from cdf.utils.s3 import list_files


class TaskTestCase(unittest.TestCase):
    @classmethod
    def get_s3_filename(cls, s3_uri, key_obj):
        return os.path.join(s3_uri, os.path.basename(key_obj.name))

    @classmethod
    def get_files(cls, s3_uri, regexp):
        return partition_aware_sort([
            cls.get_s3_filename(s3_uri, k) for k in
            list_files(s3_uri, regexp)
        ])