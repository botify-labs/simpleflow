import unittest
from cdf.core.streams.base import StreamDefBase
from cdf.query.datamodel import get_document_fields_from_features_options, _render_field_to_end_user
from cdf.query.constants import FLAG_STRING_URL
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
                FLAG_STRING_URL
            }
        },
        "depth": {
            "verbose_name": "Depth",
            "type": "integer",
            "group": "metrics"
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
                "type": "url",
                "is_sortable": True,
                "group": "main_group",
                "multiple": False,
                "searchable": True
            }
        )

        self.assertEquals(
            _render_field_to_end_user(CustomStreamDef, "depth"),
            {
                "value": "depth",
                "name": "Depth",
                "type": "integer",
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
        fields = get_document_fields_from_features_options({"main": {"lang": True}})
        self.assertTrue('lang' in [k[0] for k in fields])

        fields = get_document_fields_from_features_options({"main": {"lang": False}})
        self.assertFalse('lang' in [k[0] for k in fields])

        fields = get_document_fields_from_features_options({"main": None})
        self.assertFalse('lang' in [k[0] for k in fields])
