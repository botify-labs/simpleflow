import unittest
from cdf.constants import parse_field_element, _PROPERTY, construct_mapping, parse_field_path, _URLS_DATA_META_MAPPING, URLS_DATA_MAPPING


class TestMappingGeneration(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parse_single_elem(self):
        result = parse_field_element('a.b.c', {1: 2})
        expected = {'a': {_PROPERTY: {'b': {_PROPERTY: {'c': {1: 2}}}}}}
        self.assertDictEqual(result, expected)

    def test_parse_field_path(self):
        path = 'a.b.c'
        expected = 'a.properties.b.properties.c'
        self.assertEqual(parse_field_path(path), expected)

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

        result = construct_mapping(meta_mapping)
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

        result = construct_mapping(meta_mapping)
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


        target = URLS_DATA_MAPPING['urls']['properties']
        result = construct_mapping(_URLS_DATA_META_MAPPING)

        for key in target:
            self.assertDictEqual(result[key], target[key])
        self.assertDictEqual(result, target)