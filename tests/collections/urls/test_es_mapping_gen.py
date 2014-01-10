import unittest
from cdf.collections.urls.es_mapping_generation import _parse_field_path, generate_es_mapping
from cdf.constants import URLS_DATA_MAPPING_DEPRECATED, _URLS_DATA_META_MAPPING


class TestMappingGeneration(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parse_field_path(self):
        path = 'a.b.c'
        result = _parse_field_path(path)
        expected = 'a.properties.b.properties.c'
        self.assertEqual(result, expected)

    def test_mapping_generation(self):
        # simple case with no-index
        meta_mapping = {
            "error_links.3xx.nb": {"type": "long"},
            "error_links.3xx.urls": {
                "type": "long",
                "settings": {
                    "no_index",
                    "list"
                }
            }
        }

        result = generate_es_mapping(meta_mapping)
        expected = {
            "error_links": {
                "properties": {
                    "3xx": {
                        "properties": {
                            "nb": {"type": "long"},
                            "urls": {"type": "long", "index": "no"}
                        }
                    }
                }
            }
        }

        self.assertDictEqual(result, expected)

        # `multi_field` case
        meta_mapping = {
            "metadata.title": {
                "type": "string",
                "settings": {
                    "include_not_analyzed",
                    "list"
                }
            },
        }

        result = generate_es_mapping(meta_mapping)
        expected = {
            "metadata": {
                "properties": {
                    "title": {
                        "type": "multi_field",
                        "fields": {
                            "title": {
                                "type": "string"
                            },
                            "untouched": {
                                "type": "string",
                                "index": "not_analyzed"
                            }
                        }
                    }
                }
            }
        }
        self.assertDictEqual(result, expected)

        target = URLS_DATA_MAPPING_DEPRECATED['urls']['properties']
        result = generate_es_mapping(_URLS_DATA_META_MAPPING)

        for key in target:
            self.assertDictEqual(result[key], target[key])
        self.assertDictEqual(result, target)