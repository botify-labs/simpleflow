import unittest
import mock

from cdf.query.constants import FIELD_RIGHTS
from cdf.metadata.url.url_metadata import ES_NO_INDEX
from cdf.core.metadata import (
    make_fields_private,
    generate_data_format
)
from cdf.core.features import Feature
from cdf.core.streams.base import StreamDefBase


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

        actual_result = make_fields_private(input_mapping)
        self.assertEquals(
            set([ES_NO_INDEX, FIELD_RIGHTS.PRIVATE]),
            actual_result["foo"]["settings"]
        )
        self.assertTrue(
            set([FIELD_RIGHTS.PRIVATE]),
            actual_result["bar"]["settings"]
        )


enable_func = lambda option: option is not None and option.get('enable', False)


class TestOneStreamDef(StreamDefBase):
    FILE = 'test_one'
    HEADERS = (
        ('id', int),
        ('url', str)
    )
    URL_DOCUMENT_DEFAULT_GROUP = 'toto'
    URL_DOCUMENT_MAPPING = {
        'url': {
            'something': 'url_configs'
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
            'enabled': enable_func
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

        expected = {
            'url': {
                'something': 'url_configs',
                'group': 'toto',
                'feature': 'feature1'
            },
            'enable': {
                'enabled': enable_func,
                'group': '',
                'feature': 'feature2'
            },
        }
        self.assertEqual(expected, data_format)

    def test_filter_feature(self):
        options = {'feature1': None}
        data_format = generate_data_format(
            feature_options=options,
            available_features=[self.feature1, self.feature2]
        )
        expected = {
            'url': {
                'something': 'url_configs',
                'group': 'toto',
                'feature': 'feature1'
            }
        }

        self.assertEqual(expected, data_format)

    def test_filter_feature_option(self):
        options = {'feature1': None, 'feature2': None}
        data_format = generate_data_format(
            feature_options=options,
            available_features=[self.feature1, self.feature2]
        )
        # feature2's `enable` field should be filter out
        # as it's not explicitly set to true in its feature option
        expected = {
            'url': {
                'something': 'url_configs',
                'group': 'toto',
                'feature': 'feature1'
            }
        }

        self.assertEqual(expected, data_format)

    def test_comparison_data_format(self):
        comparison_key = 'comparison'
        comparison_feature = Feature(
            comparison_key, comparison_key, None, None)
        prevous_options = {'options': {'feature1': None}}
        options = {'feature1': None, comparison_key: prevous_options}
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
                'something': 'url_configs',
                'group': 'toto',
                'feature': 'feature1'
            },
            'previous.url': {
                'something': 'url_configs',
                'group': 'previous.toto',
                'feature': 'feature1'
            },
            'previous_exists': {
                'default_value': None, 'type': 'boolean'
            },
            'disappeared': {
                'default_value': None, 'type': 'boolean'
            }
        }
        self.assertEqual(expected, data_format)