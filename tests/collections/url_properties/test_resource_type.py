# -*- coding:utf-8 -*-
import unittest
import logging


from cdf.log import logger
from cdf.collections.url_properties.resource_type import compile_resource_type_settings, ResourceTypeSettingsValidator

logger.setLevel(logging.DEBUG)


class TestUrlPropertiesValidator(unittest.TestCase):

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
        v = ResourceTypeSettingsValidator(settings)
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
        v = ResourceTypeSettingsValidator(settings)
        self.assertFalse(v.is_valid())

    def test_missing_field(self):
        settings = {
            'www.site.*': [
                {
                    'query': 'STARTS(path, "/test/")',
                }
            ]
        }
        v = ResourceTypeSettingsValidator(settings)
        self.assertFalse(v.is_valid())
        self.assertEquals(len(v.field_errors), 1)

    def test_bad_request(self):
        settings = {
            'www.site.com': [
                {
                    'query': 'XX(path, "/test/")',
                    'value': 'test'
                }
            ]
        }
        v = ResourceTypeSettingsValidator(settings)
        self.assertFalse(v.is_valid())
        self.assertEquals(len(v.query_errors), 1)

    def test_bad_field(self):
        settings = {
            'www.site.*': [
                {
                    'any_field': 'test',
                    'value': 'test'
                }
            ]
        }
        v = ResourceTypeSettingsValidator(settings)
        self.assertFalse(v.is_valid())
        # 2 errors : `query` is missing and `any_field` is not recognized'
        self.assertEquals(len(v.field_errors), 2)

    def test_compiled(self):
        settings = {
            'www.site.com': [
                {
                    'query': 'STARTS(path, "/test")',
                    'value': 'test',
                    'rule_id': 'test'
                },
                {
                    'query': 'ENDS(path, ".json")',
                    'value': 'json',
                    'inherits_from': 'test'
                },
            ]
        }
        v = compile_resource_type_settings(settings)
        expected_items = [
            [('rule_id', 'test'), ('value', 'test')],
            [('value', 'json'), ('inherits_from', 'test')]
        ]

        # We don't test the query because of the lambda value
        def clean_query(result):
            return filter(lambda i: i[0] != 'query', result)
        returned_items = [clean_query(k.items()) for k in v]

        self.assertItemsEqual(expected_items[0], returned_items[0])
        self.assertItemsEqual(expected_items[1], returned_items[1])
