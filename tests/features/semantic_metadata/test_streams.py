import unittest

from cdf.core.metadata.constants import RENDERING, FIELD_RIGHTS
from cdf.metadata.url.url_metadata import (
    DIFF_QUANTITATIVE,
    DIFF_QUALITATIVE
)
from cdf.features.semantic_metadata.streams import (
    _get_duplicate_document_mapping
)


class TestGetDuplicateDocumentMapping(unittest.TestCase):
    def test_nominal_case(self):
        metadata_list = ["title"]
        duplicate_type = "foo_duplicate"
        verbose_prefix = "foo"
        order_seed = 100
        actual_result = _get_duplicate_document_mapping(
            metadata_list,
            duplicate_type,
            verbose_prefix,
            order_seed
        )
        expected_result = {
            'metadata.title.foo_duplicate.nb': {
                'type': 'integer',
                'verbose_name': 'Number of foo duplicate Title',
                'settings': {
                    'es:doc_values',
                    'agg:categorical',
                    'agg:numerical',
                    FIELD_RIGHTS.FILTERS,
                    FIELD_RIGHTS.SELECT,
                    DIFF_QUANTITATIVE
                }
            },
            'metadata.title.foo_duplicate.is_first': {
                'type': 'boolean',
                'verbose_name':
                'First foo duplicate Title found',
                'settings': {
                    FIELD_RIGHTS.SELECT,
                    FIELD_RIGHTS.FILTERS,
                    DIFF_QUALITATIVE
                }
            },
            'metadata.title.foo_duplicate.urls': {
                'type': 'integer',
                'verbose_name': 'Pages with the same foo Title',
                'settings': {
                    'es:no_index',
                    'url_id',
                    'list',
                    RENDERING.URL_STATUS,
                    FIELD_RIGHTS.SELECT
                }
            },
            'metadata.title.foo_duplicate.urls_exists': {
                'default_value': None, 'type': 'boolean'
            }
        }
        self.assertEqual(expected_result, actual_result)
