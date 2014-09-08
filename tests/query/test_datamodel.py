import unittest
import mock
from cdf.core.features import Feature

from cdf.core.streams.base import StreamDefBase
from cdf.query.datamodel import (
    get_fields,
    get_groups,
)
from cdf.query.constants import RENDERING, FIELD_RIGHTS
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
            "settings": {
                LIST,
                FIELD_RIGHTS.FILTERS
            }
        },
        "content_same_urls": {
            "verbose_name": "Contents with the same url",
            "type": "string",
            "settings": {
                LIST,
                ES_NO_INDEX,
                FIELD_RIGHTS.FILTERS_EXIST,
                FIELD_RIGHTS.SELECT
            }
        }
    }


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
        pass


class ComparisonTestCase(unittest.TestCase):
    def test_previous(self):
        # current crawl : feature main, links and comparison are enabled
        # previous crawl : only main is enabled
        fields = get_fields(
            {"main": None,
             "links": None,
             # TODO it's better to name it `feature_options` for consistency
             "comparison": {"options": {"main": None}}}
        )
        fields_configs = [f['value'] for f in fields]
        self.assertIn('url', fields_configs)
        self.assertIn('previous.url', fields_configs)
        # links not enabled on the previous crawl
        self.assertNotIn('previous.outlinks_internal.nb.total', fields_configs)

        fields_verbose = [f['name'] for f in fields]
        self.assertIn('Previous Http Code', fields_verbose)
