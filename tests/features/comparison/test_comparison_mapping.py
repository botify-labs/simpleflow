import unittest
from cdf.features.comparison.tasks import get_comparison_mapping


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
        result = get_comparison_mapping(self.mapping, self.extras)
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

