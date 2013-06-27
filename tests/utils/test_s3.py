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
        access_key, secret_key, bucket, location = uri_parse('s3://my_access_key@my_secret_key:bucket/location')
        self.assertEquals(access_key, 'my_access_key')
        self.assertEquals(secret_key, 'my_secret_key')
        self.assertEquals(bucket, 'bucket')
        self.assertEquals(location, 'location')
