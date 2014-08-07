import unittest
from cdf.features.comparison.tasks import get_comparison_data_format
from cdf.metadata.url.es_backend_utils import ElasticSearchBackend


class TestComparisonMapping(unittest.TestCase):
    mapping = {
        'outer.inner': {'type': 'boolean'},
        'exists': {'type': 'boolean'}
    }

    extras = {
        'previous_exists': {'type': 'boolean'},
        'disappeared': {'type': 'boolean'}
    }

    def test_comparison_mapping(self):
        format = get_comparison_data_format(self.mapping, self.extras)
        result = ElasticSearchBackend(format).mapping()
        expected = {
            'outer': {
                'properties': {
                    'inner': {
                        'type': 'boolean',
                    }
                }
            },
            'exists': {
                'type': 'boolean'
            },
            'previous_exists': {
                'type': 'boolean'
            },
            'disappeared': {
                'type': 'boolean'
            },
            'previous': {
                'properties': {
                    'outer': {
                        'properties': {
                            'inner': {
                                'type': 'boolean',
                            }
                        }
                    },
                    'exists': {
                        'type': 'boolean'
                    },
                }
            }
        }

        self.assertEqual(result['urls']['properties'], expected)

