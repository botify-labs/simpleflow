import unittest
from cdf.core.streams.base import StreamDefBase
from cdf.query.datamodel import (
    get_fields,
    get_groups,
    _render_field_to_end_user
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

    def test_end_user_field(self):
        self.assertEquals(
            _render_field_to_end_user(CustomStreamDef, "url"),
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
            _render_field_to_end_user(CustomStreamDef, "delay"),
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
        self.assertTrue(_render_field_to_end_user(CustomStreamDef, "content")["multiple"])
        # `content` field can be filtered but no returned in the results
        self.assertEquals(_render_field_to_end_user(CustomStreamDef, "content")["rights"], ["filters"])

        # `content_same_urls` field can be filtered but only with `exists` check and  returned in the results
        self.assertEquals(_render_field_to_end_user(CustomStreamDef, "content_same_urls")["rights"], ["filters_exist", "select"])

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


class ComparisonTestCase(unittest.TestCase):

    def test_previous(self):
        # current crawl : feature main, links and comparison are enabled
        # previous crawl : only main is enabled
        fields = get_fields({"main": None, "links": None, "comparison": {"options": {"main": None}}})
        fields = [f['value'] for f in fields]
        self.assertTrue('url' in fields)
        self.assertTrue('previous.url' in fields)
        # links not enabled on the previous crawl
        self.assertTrue('previous.outlinks_internal.nb.total' not in fields)
