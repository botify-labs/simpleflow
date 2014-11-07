import unittest
import mock
from cdf.core.features import Feature

from cdf.core.streams.base import StreamDefBase
from cdf.query.datamodel import (
    get_fields,
    get_groups,
    _get_field_rights,
    _data_model_sort_key,
    _get_group_sort_key
)
from cdf.core.metadata.constants import RENDERING, FIELD_RIGHTS
from cdf.metadata.url.url_metadata import LIST, ES_NO_INDEX


class CustomStreamDef(StreamDefBase):
    FILE = 'test'
    HEADERS = (
        ('id', int),
        ('url', str)
    )
    URL_DOCUMENT_DEFAULT_GROUP = "main_group"
    URL_DOCUMENT_MAPPING = {
        "url": {
            "verbose_name": "Url",
            "type": "string",
            "order": 1,  # rank 1
            "settings": {
                RENDERING.URL
            }
        },
        "delay": {
            "verbose_name": "Delay",
            "type": "integer",
            "group": "metrics",
            "settings": {
                RENDERING.TIME_SEC,
                FIELD_RIGHTS.SELECT
            }
        },
        "content": {
            "verbose_name": "Contents",
            "type": "string",
            "order": 1000,  # rank 3
            "settings": {
                LIST,
                FIELD_RIGHTS.FILTERS
            }
        },
        "content_same_urls": {
            "verbose_name": "Contents with the same url",
            "type": "string",
            "order": 5,  # rank 2
            "settings": {
                LIST,
                ES_NO_INDEX,
                FIELD_RIGHTS.FILTERS_EXIST,
                FIELD_RIGHTS.SELECT
            }
        },
        "private": {
            "type": "string",
            "settings": {
                FIELD_RIGHTS.PRIVATE
            }
        },
        "admin": {
            "type": "string",
            "settings": {
                FIELD_RIGHTS.ADMIN
            }
        }
    }


class TestDataModelSortKey(unittest.TestCase):
    def test_nominal_case(self):
        elem = (11, {"group": "foo", "order": 10, "verbose_name": "bar"})
        self.assertEqual(_data_model_sort_key(elem), ("foo", "bar"))

    def test_missing_group(self):
        elem = (11, {"order": 10, "verbose_name": "bar"})
        self.assertEqual(_data_model_sort_key(elem), ("", "bar"))

    def test_missing_verbose_name(self):
        elem = (11, {"group": "foo"})
        self.assertEqual(_data_model_sort_key(elem), ("foo", ""))

    def test_main_group(self):
        elem = (11, {"group": "main", "order": 10, "verbose_name": "bar"})
        #main group should appear first.
        #Thus its key should be empty
        self.assertEqual(_data_model_sort_key(elem), ("", "bar"))


class TestGetGroupSortKey(unittest.TestCase):
    def test_nominal_case(self):
        self.assertEqual("foo", _get_group_sort_key("foo"))

    def test_main_case(self):
        self.assertEqual("", _get_group_sort_key("main"))


class FieldsTestCase(unittest.TestCase):
    def setUp(self):
        self.feature1 = Feature('feature1', 'feature1', None, None)
        # mock stream_def in feature
        self.feature1.get_streams_def = mock.Mock(return_value=[CustomStreamDef])
        self.features = [self.feature1]

    def test_harness(self):
        data_model = get_fields({'feature1': None},
                                available_features=self.features)
        data_model = {k['value']: k for k in data_model}
        self.assertEquals(
            data_model['url'],
            {
                "value": "url",
                "name": "Url",
                "data_type": "string",
                "field_type": "url",
                "is_sortable": True,
                "group": "main_group",
                "multiple": False,
                "rights": ["filters", "select"]
            }
        )

        self.assertEquals(
            data_model['delay'],

            {
                "value": "delay",
                "name": "Delay",
                "data_type": "integer",
                "field_type": "time_sec",
                "is_sortable": True,
                "group": "metrics",
                "multiple": False,
                "rights": ["select"]
            }
        )

        # `content` field is `multiple`
        self.assertTrue(data_model['content']["multiple"])
        # `content` field can be filtered but no returned in the results
        self.assertEquals(data_model["content"]["rights"], ["filters"])

        # `content_same_urls` field can be filtered
        # but only with `exists` check and  returned in the results
        self.assertEquals(
            data_model['content_same_urls']["rights"],
            ["filters_exist", "select"]
        )

    def test_enabled(self):
        fields = get_fields({"main": {"lang": True}})
        self.assertTrue('Lang' in [k["name"] for k in fields])

        fields = get_fields({"main": {"lang": False}})
        self.assertFalse('lang' in [k["name"] for k in fields])

        fields = get_fields({"main": None})
        self.assertFalse('lang' in [k["name"] for k in fields])

    def test_groups(self):
        groups = get_groups({"main": {"lang": True}})
        self.assertEquals(
            [g['id'] for g in groups],
            ['scheme', 'main']
        )

    def test_ordering(self):
        data_model = get_fields({'feature1': None},
                                available_features=self.features)
        data_model = [k['value'] for k in data_model]
        # datamodel should be sorted within each field
        expected = [
            'content',
            'content_same_urls',
            'url',
            'delay'  # delay is in an other group
        ]
        self.assertEqual(expected, data_model)

    def test_private(self):
        data_model = get_fields(
            {'feature1': None},
            available_features=self.features
        )
        data_model = [k['value'] for k in data_model]
        # by default `private` is excluded
        self.assertFalse("private" in data_model)

        data_model = get_fields(
            {'feature1': None},
            remove_private=False,
            available_features=self.features
        )
        data_model = [k['value'] for k in data_model]
        self.assertTrue("private" in data_model)

    def test_admin(self):
        data_model = get_fields(
            {'feature1': None},
            available_features=self.features
        )
        data_model = [k['value'] for k in data_model]
        # by default `admin` is excluded
        self.assertFalse("admin" in data_model)

        data_model = get_fields(
            {'feature1': None},
            remove_admin=False,
            available_features=self.features
        )
        data_model = [k['value'] for k in data_model]
        self.assertTrue("admin" in data_model)


class TestGetFieldRights(unittest.TestCase):
    def test_nominal_case(self):
        settings = {LIST, FIELD_RIGHTS.SELECT}
        actual_result = _get_field_rights(settings)
        expected_result = ["select"]
        self.assertItemsEqual(expected_result, actual_result)

    def test_default_value(self):
        settings = {LIST}
        actual_result = _get_field_rights(settings)
        expected_result = ["select", "filters"]
        self.assertItemsEqual(expected_result, actual_result)

    def test_admin_field(self):
        settings = {LIST, FIELD_RIGHTS.ADMIN, FIELD_RIGHTS.SELECT}
        actual_result = _get_field_rights(settings)
        expected_result = ["admin", "select"]
        self.assertItemsEqual(expected_result, actual_result)

    def test_default_value_admin_field(self):
        settings = {LIST, FIELD_RIGHTS.ADMIN}
        actual_result = _get_field_rights(settings)
        expected_result = ["admin", "select", "filters"]
        self.assertItemsEqual(expected_result, actual_result)


class ComparisonTestCase(unittest.TestCase):
    def setUp(self):
        self.feature_options = {
            "main": None,
            "links": None,
            # TODO it's better to name it `feature_options` for consistency
            "comparison": {"options": {"main": None, "links": None}}
        }

    def test_fields(self):
        # current crawl : feature main, links and comparison are enabled
        # previous crawl : only main is enabled
        fields = get_fields(self.feature_options)
        fields_configs = [f['value'] for f in fields]
        self.assertIn('url', fields_configs)
        self.assertIn('previous.url', fields_configs)
        # `main_image` not enabled on the previous crawl
        self.assertNotIn('previous.main_image', fields_configs)

        fields_verbose = [f['name'] for f in fields]
        self.assertIn('Previous Http Code', fields_verbose)

    def test_groups(self):
        # current crawl : feature main, links and comparison are enabled
        # previous crawl : only main is enabled
        groups = get_groups(self.feature_options)
        names = [g['id'] for g in groups]

        self.assertIn('inlinks', names)
        self.assertIn('previous.inlinks', names)
        self.assertIn('diff.inlinks', names)

    def test_groups_non_comparison(self):
        # current crawl : feature main, links and comparison are enabled
        # previous crawl : only main is enabled
        groups = get_groups({'links': None})
        names = [g['id'] for g in groups]

        self.assertIn('inlinks', names)
        self.assertNotIn('previous.inlinks', names)
        self.assertNotIn('diff.inlinks', names)
