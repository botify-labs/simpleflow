import unittest
import mock

from cdf.core.features import (
    generate_data_format
)
from cdf.core.features import Feature
from cdf.core.streams.base import StreamDefBase


class TestOneStreamDef(StreamDefBase):
    FILE = 'test_one'
    HEADERS = (
        ('id', int),
        ('url', str)
    )
    URL_DOCUMENT_MAPPING = {
        'url': {
            'something': 'url_configs'
        },
        'delay': {
            'something': 'delay_configs'
        }
    }


class TestTwoStreamDef(StreamDefBase):
    FILE = 'test_two'
    HEADERS = (
        ('id', int),
        ('url', str)
    )
    URL_DOCUMENT_MAPPING = {
        'enable': {
            'enabled': lambda option: option is not None and option.get('enable', False)
        },
    }


class TestDataFormatGeneration(unittest.TestCase):
    def setUp(self):
        self.feature1 = Feature('feature1', 'feature1', None, None)
        self.feature2 = Feature('feature2', 'feature2', None, None)
        # mock stream_def in feature
        self.feature1.get_streams_def = mock.Mock(return_value=[TestOneStreamDef])
        self.feature2.get_streams_def = mock.Mock(return_value=[TestTwoStreamDef])

        self.features = [self.feature1, self.feature2]

    def test_harness(self):
        options = {'feature1': None, 'feature2': {'enable': True}}
        data_format = generate_data_format(
            feature_options=options,
            available_features=[self.feature1, self.feature2]
        )
        expected = TestOneStreamDef.URL_DOCUMENT_MAPPING.copy()
        expected.update(TestTwoStreamDef.URL_DOCUMENT_MAPPING)

        self.assertEqual(expected, data_format)

    def test_filter_feature(self):
        options = {'feature1': None}
        data_format = generate_data_format(
            feature_options=options,
            available_features=[self.feature1, self.feature2]
        )
        expected = TestOneStreamDef.URL_DOCUMENT_MAPPING

        self.assertEqual(expected, data_format)

    def test_filter_feature_option(self):
        options = {'feature1': None, 'feature2': None}
        data_format = generate_data_format(
            feature_options=options,
            available_features=[self.feature1, self.feature2]
        )
        # feature2's `enable` field should be filter out
        # as it's not explicitly set to true in its feature option
        expected = TestOneStreamDef.URL_DOCUMENT_MAPPING

        self.assertEqual(expected, data_format)

    def test_comparison_data_format(self):
        comparison_key = 'comparison'
        comparison_feature = Feature(
            comparison_key, comparison_key, None, None)
        options = {'feature1': None, comparison_key: None}
        data_format = generate_data_format(
            feature_options=options,
            available_features=[
                self.feature1,
                self.feature2,
                comparison_feature
            ]
        )
        expected = {
            'url': {
                'something': 'url_configs'
            },
            'delay': {
                'something': 'delay_configs'
            },
            'previous.url': {
                'something': 'url_configs'
            },
            'previous.delay': {
                'something': 'delay_configs'
            },
            'previous_exists': {
                'default_value': None, 'type': 'boolean'
            },
            'disappeared': {
                'default_value': None, 'type': 'boolean'
            }
        }
        self.assertEqual(expected, data_format)