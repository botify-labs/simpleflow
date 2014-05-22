import unittest
from cdf.core.streams.base import StreamDefBase
from cdf.query.datamodel import (
    get_fields,
    get_groups,
    _render_field_to_end_user
)
from cdf.query.constants import RENDERING
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
                RENDERING.TIME_SEC
            }
        },
        "content": {
            "verbose_name": "Contents",
            "type": "string",
            "settings": {
                LIST, ES_NO_INDEX
            }
        }
    }


class FieldsTestCase(unittest.TestCase):
    def setUp(self):
        pass

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
                "searchable": True
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
                "searchable": True,
                "multiple": False
            }
        )

        # `content` field is `multiple`
        self.assertTrue(_render_field_to_end_user(CustomStreamDef, "content")["multiple"])
        # `content` field is NOT `searchable`
        self.assertFalse(_render_field_to_end_user(CustomStreamDef, "content")["searchable"])

    def test_enabled(self):
        fields = get_fields({"main": {"lang": True}})
        self.assertTrue('lang' in [k["name"] for k in fields])

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
