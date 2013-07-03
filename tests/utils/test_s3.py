import unittest
import logging

from cdf.log import logger
from cdf.utils.s3 import uri_parse

logger.setLevel(logging.DEBUG)


class TestCaster(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_uri_parse(self):
        bucket, location = uri_parse('s3://bucket/location/sublocation')
        self.assertEquals(bucket, 'bucket')
        self.assertEquals(location, 'location/sublocation')
