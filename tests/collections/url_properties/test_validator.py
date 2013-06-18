# -*- coding:utf-8 -*-
import unittest
import logging


from cdf.log import logger
from cdf.collections.url_properties.validator import ResourceTypeValidator

logger.setLevel(logging.DEBUG)


class TestUrlDataGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple(self):
        settings = {
            '*.site.com': [
                {
                    'query': 'STARTS(path, "/test/")',
                    'value': 'test'
                }
            ]
        }
        v = ResourceTypeValidator(settings)
        self.assertTrue(v.is_valid())

    def test_bad_hosts(self):
        settings = {
            'www.site.*': [
                {
                    'query': 'STARTS(path, "/test/")',
                    'value': 'test'
                }
            ]
        }
        v = ResourceTypeValidator(settings)
        self.assertFalse(v.is_valid())

    def test_missing_field(self):
        settings = {
            'www.site.*': [
                {
                    'query': 'STARTS(path, "/test/")',
                }
            ]
        }
        v = ResourceTypeValidator(settings)
        self.assertFalse(v.is_valid())
        self.assertEquals(len(v.field_errors), 1)

    def test_bad_request(self):
        settings = {
            'www.site.*': [
                {
                    'query': 'XX(path, "/test/")',
                    'value': 'test'
                }
            ]
        }
        v = ResourceTypeValidator(settings)
        self.assertFalse(v.is_valid())
        logger.info(v.query_errors)
        self.assertEquals(len(v.query_errors), 1)
