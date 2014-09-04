import unittest

from cdf.query.constants import RENDERING
from cdf.features.semantic_metadata.streams import (
    _get_duplicate_document_mapping,
    _make_fields_private
)
from cdf.metadata.url.url_metadata import ES_NO_INDEX
from cdf.query.constants import FIELD_RIGHTS


class TestGetDuplicateDocumentMapping(unittest.TestCase):
    def test_nominal_case(self):
        metadata_list = ["title"]
        duplicate_type = "foo_duplicate"
        verbose_duplicate_type = "foo duplicate"
        order_seed = 100
        actual_result = _get_duplicate_document_mapping(metadata_list,
                                                        duplicate_type,
                                                        verbose_duplicate_type,
                                                        order_seed)
        expected_result = {
            'metadata.title.foo_duplicate.nb': {
                'type': 'integer', 'verbose_name': 'Number of foo duplicate Title', 'order': 100, 'settings': set(['es:doc_values', 'agg:categorical', 'agg:numerical'])
            },
            'metadata.title.foo_duplicate.is_first': {
                'type': 'boolean', 'verbose_name': 'First foo duplicate Title found', 'order': 120
            },
            'metadata.title.foo_duplicate.urls': {
                'type': 'integer', 'verbose_name': 'Pages with the same Title', 'order': 110, 'settings': set(['es:no_index', 'url_id', 'list', RENDERING.URL_STATUS, FIELD_RIGHTS.SELECT])
            },
            'metadata.title.foo_duplicate.urls_exists': {
                'default_value': None, 'type': 'boolean'
            }
        }
        self.assertEqual(expected_result, actual_result)


class TestMakeFieldsPrivate(unittest.TestCase):
    def test_nominal_case(self):
        input_mapping = {
            "foo": {
                "verbose_name": "I am foo",
                "settings": set([ES_NO_INDEX, FIELD_RIGHTS.SELECT])
            },
            "bar": {
                "verbose_name": "I am bar",
            }
        }

        actual_result = _make_fields_private(input_mapping)
        self.assertEquals(
            set([ES_NO_INDEX, FIELD_RIGHTS.SELECT, FIELD_RIGHTS.PRIVATE]),
            actual_result["foo"]["settings"]
        )
        self.assertTrue(
            set([FIELD_RIGHTS.PRIVATE]),
            actual_result["bar"]["settings"]
        )
