# -*- coding:utf-8 -*-
import unittest
import logging


from cdf.log import logger
from cdf.collections.urls.tagging.resource_type import (compile_resource_type_settings,
                                                        validate_resource_type_settings,
                                                        ResourceTypeSettingsException)

logger.setLevel(logging.DEBUG)


class TestResourceTypeSettings(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple(self):
        settings = [{
            'host': '*.site.com',
            'rules': [{
                'query': 'STARTS(path, "/test/")',
                'value': 'test'
            }]
        }]
        self.assertTrue(validate_resource_type_settings(settings))

    def test_bad_struct(self):
        settings = {}

        # Should start with a list, not a json object
        settings[0] = {
            'host': '*.site.com'
        }

        # forgotten 'rules'
        settings[1] = [
            {
                'host': '*.site.com'
            }
        ]

        # unkown field
        settings[2] = [
            {
                'host': '*.site.com',
                'rules': [
                    {'query': 'path == "/test.html"',
                     'value': 'test'}
                ],
                'xxx': 'yy'
            }
        ]

        for setting in settings.itervalues():
            try:
                validate_resource_type_settings(setting)
                self.assertFail()
            except ResourceTypeSettingsException as inst:
                self.assertEquals(len(inst.format_errors), 1)

    def test_bad_hosts(self):
        settings = {}

        settings[0] = [{
            'host': 'www.*.com',
            'rules': [{
                'query': 'STARTS(path, "/test/")',
                'value': 'test'
            }]
        }]

        # host is not a string
        settings[1] = [{
            'host': 5,
            'rules': [{
                'query': 'STARTS(path, "/test/")',
                'value': 'test'
            }]
        }]

        for setting in settings.itervalues():
            try:
                validate_resource_type_settings(setting)
                self.assertFail()
            except ResourceTypeSettingsException as inst:
                self.assertEquals(len(inst.host_errors), 1)

    def test_bad_rules_type(self):
        settings = [{
            'host': 'www.site.com',
            'rules': {}
        }]

        try:
            validate_resource_type_settings(settings)
            self.assertFail()
        except ResourceTypeSettingsException as inst:
            self.assertEquals(len(inst.format_errors), 1)

    def test_missing_field(self):
        settings = [{
            'host': 'www.*.com',
            'rules': [{
                'query': 'STARTS(path, "/test/")',
            }]
        }]

        try:
            validate_resource_type_settings(settings)
            self.assertFail()
        except ResourceTypeSettingsException as inst:
            self.assertEquals(len(inst.field_errors), 1)

    def test_bad_request(self):
        settings = [{
            'host': 'www.site.com',
            'rules': [{
                'query': 'XXX(path, "/test/")',
                'value': 'test'
            }]
        }]

        try:
            validate_resource_type_settings(settings)
            self.assertFail()
        except ResourceTypeSettingsException as inst:
            self.assertEquals(len(inst.query_errors), 1)

    def test_bad_field(self):
        settings = [{
            'host': '*.site.com',
            'rules': [{
                'any_field': 'test',
                'value': 'test'
            }]
        }]

        try:
            validate_resource_type_settings(settings)
            self.assertFail()
        except ResourceTypeSettingsException as inst:
            # 2 errors : `query` is missing and `any_field` is not recognized'
            self.assertEquals(len(inst.field_errors), 2)

    def test_bad_inherits_from(self):
        """
        Second rule does not inherits from an existing url_id
        """

        settings = [{
            'host': 'www.site.com',
            'rules': [
                {
                    'query': 'STARTS(path, "/test")',
                    'value': 'test',
                    'rule_id': 'test'
                },
                {
                    'query': 'ENDS(path, ".json")',
                    'value': 'json',
                    'inherits_from': 'test2'
                },
            ]
        }]

        try:
            validate_resource_type_settings(settings)
            self.assertFail()
        except ResourceTypeSettingsException as inst:
            self.assertEquals(len(inst.inheritance_errors), 1)

    def test_bad_inherits_rule_id_same_value(self):
        """
        Second rule does not inherits from an existing url_id
        """

        settings = [{
            'host': 'www.site.com',
            'rules': [
                {
                    'query': 'STARTS(path, "/test")',
                    'value': 'test',
                    'rule_id': 'test',
                    'inherits_from': 'test'
                }
            ]
        }]

        try:
            validate_resource_type_settings(settings)
            self.assertFail()
        except ResourceTypeSettingsException as inst:
            self.assertEquals(len(inst.inheritance_errors), 1)

    def test_abstract_with_value_field(self):
        # `abtract` cannot come with `value` field
        settings = [{
            'host': 'www.site.com',
            'rules': [
                {
                    'query': 'STARTS(path, "/test")',
                    'value': 'test',
                    'abstract': True,
                    'rule_id': 'test'
                }
            ]
        }]

        try:
            validate_resource_type_settings(settings)
            self.assertFail()
        except ResourceTypeSettingsException as inst:
            self.assertEquals(len(inst.field_errors), 1)

    def test_abstract_missing_rule_id(self):
        # `abtract` cannot be set to `True` without `rule_id` field
        settings = [{
            'host': 'www.site.com',
            'rules': [
                {
                    'query': 'STARTS(path, "/test")',
                    'abstract': True,
                }
            ]
        }]

        try:
            validate_resource_type_settings(settings)
            self.assertFail()
        except ResourceTypeSettingsException as inst:
            self.assertEquals(len(inst.field_errors), 1)

        # But it passes when `abstract` is set to `False`
        settings = [{
            'host': 'www.site.com',
            'rules': [
                {
                    'query': 'STARTS(path, "/test")',
                    'abstract': False,
                    'value': 'test'
                }
            ]
        }]
        self.assertTrue(validate_resource_type_settings(settings))

    def test_compiled(self):
        settings = [{
            'host': 'www.site.com',
            'rules': [
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
        }]
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
