import unittest

from cdf.query.constants import RENDERING
from cdf.features.semantic_metadata.streams import _get_duplicate_document_mapping


class TestGetDuplicateDocumentMapping(unittest.TestCase):
    def test_nominal_case(self):
        duplicate_type = "foo_duplicate"
        verbose_duplicate_type = "foo duplicate"
        order_seed = 100
        private = False
        actual_result = _get_duplicate_document_mapping(duplicate_type,
                                                        verbose_duplicate_type,
                                                        order_seed,
                                                        private)
        expected_result = {
            'metadata.h1.foo_duplicate.is_first': {
                'type': 'boolean', 'verbose_name': 'First foo duplicate H1 found', 'order': 122
            },
            'metadata.title.foo_duplicate.nb': {
                'type': 'integer', 'verbose_name': 'Number of foo duplicate Title', 'order': 100, 'settings': set(['es:doc_values', 'agg:categorical', 'agg:numerical'])
            },
            'metadata.h1.foo_duplicate.nb': {
                'type': 'integer', 'verbose_name': 'Number of foo duplicate H1', 'order': 102, 'settings': set(['es:doc_values', 'agg:categorical', 'agg:numerical'])
            },
            'metadata.title.foo_duplicate.is_first': {
                'type': 'boolean', 'verbose_name': 'First foo duplicate Title found', 'order': 120
            },
            'metadata.title.foo_duplicate.urls': {
                'type': 'integer', 'verbose_name': 'Pages with the same Title', 'order': 110, 'settings': set(['es:no_index', 'url_id', 'list', RENDERING.URL_STATUS, FIELD_RIGHTS.SELECT])
            },
            'metadata.description.foo_duplicate.urls_exists': {
                'default_value': None, 'type': 'boolean'
            },
            'metadata.h1.foo_duplicate.urls': {
                'type': 'integer', 'verbose_name': 'Pages with the same H1', 'order': 112, 'settings': set(['es:no_index', 'url_id', 'list', RENDERING.URL_STATUS])
            },
            'metadata.description.foo_duplicate.urls': {
                'type': 'integer', 'verbose_name': 'Pages with the same Description', 'order': 111, 'settings': set(['es:no_index', 'url_id', 'list', RENDERING.URL_STATUS])
            },
            'metadata.h1.foo_duplicate.urls_exists': {
                'default_value': None, 'type': 'boolean'
            },
            'metadata.description.foo_duplicate.nb': {
                'type': 'integer', 'verbose_name': 'Number of foo duplicate Description', 'order': 101, 'settings': set(['es:doc_values', 'agg:categorical', 'agg:numerical'])
            },
            'metadata.description.foo_duplicate.is_first': {
                'type': 'boolean', 'verbose_name': 'First foo duplicate Description found', 'order': 121
            },
            'metadata.title.foo_duplicate.urls_exists': {
                'default_value': None, 'type': 'boolean'
            }
        }
        self.assertEqual(expected_result, actual_result)
