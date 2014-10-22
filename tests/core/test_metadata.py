import unittest
import mock

from cdf.core.metadata.constants import FIELD_RIGHTS
from cdf.core.metadata.dataformat import (
    generate_data_format,
    set_visibility
)
from cdf.features.comparison.streams import (
    get_previous_data_format,
    get_diff_data_format,
    EXTRA_FIELDS_FORMAT
)
from cdf.core.features import Feature
from cdf.core.streams.base import StreamDefBase
from cdf.metadata.url.es_backend_utils import ElasticSearchBackend
from cdf.metadata.url.url_metadata import (
    DIFF_QUALITATIVE, DIFF_QUANTITATIVE,
    ES_NOT_ANALYZED, ES_NO_INDEX
)


class TestSetVisibility(unittest.TestCase):
    def test_harness(self):
        input_mapping = {
            "foo": {
                "verbose_name": "I am foo",
                "settings": {ES_NO_INDEX, FIELD_RIGHTS.SELECT}
            },
            "bar": {
                "verbose_name": "I am bar",
            }
        }
        result = set_visibility(input_mapping, FIELD_RIGHTS.ADMIN)
        self.assertEquals(
            {ES_NO_INDEX, FIELD_RIGHTS.SELECT, FIELD_RIGHTS.ADMIN},
            result["foo"]["settings"]
        )
        self.assertTrue(
            {FIELD_RIGHTS.ADMIN},
            result["bar"]["settings"]
        )

    def test_wrong_visibility_flag(self):
        # not a `FIELD_RIGHTS` object, should raise
        self.assertRaises(Exception, set_visibility, {}, "wrong flag")


# Have to set a lambda here since we need to check equality
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
        }
        expected.update(EXTRA_FIELDS_FORMAT)
        self.assertEqual(expected, data_format)


class TestComparisonMapping(unittest.TestCase):
    mapping = {
        'outer.inner': {
            'type': 'boolean',
            'group': 'important'
        },
        'exists': {'type': 'boolean'}
    }

    extras = {
        'previous_exists': {'type': 'boolean'},
        'disappeared': {'type': 'boolean'}
    }

    def test_group(self):
        format = get_previous_data_format(self.mapping, {})
        group_key = 'group'
        expected_group = 'previous.important'
        result = format['previous.outer.inner'][group_key]

        self.assertEqual(result, expected_group)
        self.assertNotIn('outer.inner', format)

    def test_comparison_mapping(self):
        format = get_previous_data_format(self.mapping, self.extras)
        result = ElasticSearchBackend(format).mapping()
        expected = {
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


class TestDiffMapping(unittest.TestCase):
    mapping = {
        'a': {
            'type': 'boolean',
            'group': 'important',
            'settings': {
                DIFF_QUALITATIVE
            }
        },
        'b': {
            'type': 'integer',
            'verbose_name': 'bbbb',
            'settings': {
                DIFF_QUANTITATIVE
            }
        },
    }

    def test_harness(self):
        result = get_diff_data_format(self.mapping)
        expected = {
            'diff.a': {
                'type': 'string',
                'group': 'Diff important',
                'settings': {
                    ES_NOT_ANALYZED
                }
            },
            'diff.b': {
                'type': 'integer',
                'verbose_name': 'Diff bbbb',
            }
        }
        self.assertEqual(result, expected)

    def test_no_diff_field(self):
        result = get_diff_data_format({
            'c': {
                'type': 'integer'
            }
        })
        self.assertEqual(result, {})