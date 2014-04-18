import unittest
from cdf.metadata.url.es_backend_utils import ElasticSearchBackend
from cdf.query.query_parsing import QueryParser
from cdf.exceptions import BotifyQueryException


_STUB_FORMAT = {
    'http_code': {
        'type': 'integer',
        'settings': {
            'agg:categorical',
            'agg:numerical',
        }
    },
    'host': {
        'type': 'string',
        'settings': {
            'agg:categorical',
        }
    },
    'metadata.title.nb': {
        'type': 'integer',
        'settings': {
            'agg:numerical'
        }
    }
}


class ParsingTestCase(unittest.TestCase):
    def setUp(self):
        self.parser = QueryParser(ElasticSearchBackend(_STUB_FORMAT))

    def assertParsingError(self, func, *args, **kwargs):
        self.assertRaises(BotifyQueryException,
                          func, *args, **kwargs)


class TestSortParsing(ParsingTestCase):
    def test_parsing(self):
        sort = ['id', {'http_code': {'order': 'desc'}}]
        result = self.parser.parse_sorts(sort).transform()
        expected = [{'id': {'ignore_unmapped': True}},
                    {'http_code': {'order': 'desc', 'ignore_unmapped': True}}]
        self.assertEqual(result, expected)

        sort = ['id', {'http_code': {'order': 'asc'}}]
        result = self.parser.parse_sorts(sort).transform()
        expected = [{'id': {'ignore_unmapped': True}},
                    {'http_code': {'order': 'asc', 'ignore_unmapped': True}}]
        self.assertEqual(result, expected)

    def test_wrong_sort_structure(self):
        # sorts should be a list
        invalid = {'sort': 'field'}
        self.assertParsingError(self.parser.parse_sorts, invalid)

        # sort should not contain ints
        invalid = ['url', 'path', 1]
        self.assertParsingError(self.parser.parse_sorts, invalid)

        # order object should be in correct structure
        invalid = [{'url': 'desc'}]
        self.assertParsingError(self.parser.parse_sorts, invalid)

        # order object should be in correct structure
        invalid = [{'url': {1, 2, 3}}]
        self.assertParsingError(self.parser.parse_sorts, invalid)

        # sort element object should contain a single mapping
        invalid = [{'url': {'order': 'desc'}, 'http_code': {'order': 'desc'}}]
        self.assertParsingError(self.parser.parse_sorts, invalid)

    def test_wrong_sort_semantic(self):
        # sort field is not a child field
        # `error_links.3xx` is a valid query field but not a target for sort
        invalid = [{'error_links.3xx': {'order': 'desc'}}]
        parsed = self.parser.parse_sorts(invalid)
        self.assertParsingError(parsed.validate)

        # order param should be `desc`
        invalid = ['url', {'path': {'order': 'hey!!'}}]
        parsed = self.parser.parse_sorts(invalid)
        self.assertParsingError(parsed.validate)


class TestFieldsParsing(ParsingTestCase):
    def test_parsing(self):
        fields = ['url', 'content_type']
        result = self.parser.parse_fields(fields).transform()
        expected = ['url', 'content_type']
        self.assertEqual(result, expected)

    def test_wrong_fields_structure(self):
        # fields should be a list of strings
        invalid = 1
        self.assertParsingError(self.parser.parse_fields, invalid)

        # fields should be a list of strings
        invalid = [1, 'url']
        self.assertParsingError(self.parser.parse_fields, invalid)

        # fields should be a list of strings
        invalid = {'url', 'path'}
        self.assertParsingError(self.parser.parse_fields, invalid)

    def test_wrong_fields_semantic(self):
        # field should be a valid one
        invalid = ['field0', 'path']
        parsed = self.parser.parse_fields(invalid)
        self.assertParsingError(parsed.validate)


class TestFilterParsing(ParsingTestCase):
    def test_parse_predicate_filter(self):
        valid = {'field': 'http_code', 'value': 200}
        expected = {'term': {'http_code': 200}}
        result = self.parser.parse_predicate_filter(valid).transform()
        self.assertDictEqual(expected, result)

    def test_parse_not_filter(self):
        valid = {'not': {'field': 'http_code', 'value': 200}}
        expected = {'not': {'term': {'http_code': 200}}}
        result = self.parser.parse_not_filter(valid).transform()
        self.assertDictEqual(expected, result)

    def test_parse_boolean_filter(self):
        # boolean filter should contain a list of other filters
        invalid = {'or': {'and': [{'field': 'http_code', 'value': 200}]}}
        self.assertParsingError(self.parser.parse_filter, invalid)

        valid = {'and': [{'field': 'http_code', 'value': 200}]}
        expected = {'and': [{'term': {'http_code': 200}}]}
        result = self.parser.parse_boolean_filter(valid).transform()
        self.assertDictEqual(expected, result)

    def test_wrong_predicate_filter_semantic(self):
        # non-list predicate on list field
        invalid = {'predicate': 'eq', 'field': 'metadata.h1', 'value': 'data'}
        parsed = self.parser.parse_predicate_filter(invalid)
        self.assertParsingError(parsed.validate)

        # list predicate on non-list field
        invalid = {'predicate': 'any.contains', 'field': 'path', 'value': 'data'}
        parsed = self.parser.parse_predicate_filter(invalid)
        self.assertParsingError(parsed.validate)

        # value is missing
        invalid = {'predicate': 'eq', 'field': 'path'}
        parsed = self.parser.parse_predicate_filter(invalid)
        self.assertParsingError(parsed.validate)

        # value is not required
        invalid = {'predicate': 'exists', 'field': 'path', 'value': 'data'}
        parsed = self.parser.parse_predicate_filter(invalid)
        self.assertParsingError(parsed.validate)

        # value is of wrong type
        invalid = {'predicate': 'eq', 'field': 'path', 'value': ['data']}
        parsed = self.parser.parse_predicate_filter(invalid)
        self.assertParsingError(parsed.validate)

        # predicate field is not a child field
        # `error_links` is a valid query field, but not valid as a target
        # of predicate
        invalid = {'predicate': 'exists', 'field': 'error_links'}
        parsed = self.parser.parse_predicate_filter(invalid)
        self.assertParsingError(parsed.validate)


class TestAggregationParsing(ParsingTestCase):
    def test_parse_single_agg(self):
        valid = {'a1': {'group': [{'distinct': {'field': 'http_code'}}],
                        'metric': 'count'}}
        parsed = self.parser.parse_aggregations(valid)
        parsed.validate()

        self.assertEquals(
            parsed.named_aggs[0].transform(),
            {'terms': {'field': 'http_code', 'size': 50, 'order': {'_term': 'asc'}}}
        )

    def test_parse_default_metric(self):
        valid = {'a1': {'group': [{'distinct': {'field': 'http_code'}}]}}
        parsed = self.parser.parse_aggregations(valid)
        parsed.validate()

    def test_parse_distinct_alias(self):
        valid = {'a1': {'group': ['host'],
                        'metric': 'count'}}
        parsed = self.parser.parse_aggregations(valid)
        parsed.validate()

    def test_parse_multiple_aggs(self):
        valid = {'a1': {'group': [{'distinct': {'field': 'http_code'}}],
                        'metric': 'count'},
                 'a2': {'group': [{'distinct': {'field': 'http_code'}}],
                        'metric': 'count'}}
        parsed = self.parser.parse_aggregations(valid)
        parsed.validate()

    def test_parse_multiple_groups(self):
        valid = {'a1': {'group': [{'distinct': {'field': 'http_code'}}, "depth"],
                        'metric': 'count'}}
        parsed = self.parser.parse_aggregations(valid)
        self.assertEquals(
            parsed.named_aggs[0].transform(),
            {
                'terms': {
                    'field': 'http_code',
                    'size': 50,
                    'order': {'_term': 'asc'}
                },
                'aggs': {
                    'subagg': {
                        'terms': {
                            'field': 'depth',
                            'size': 50,
                            'order': {'_term': 'asc'}
                        }
                    }
                }
            }
        )

    def test_parse_unknown_group_op(self):
        invalid = {'a1': {'group': [{'unknown': {'field': 'http_code'}}],
                          'metric': 'count'}}
        self.assertParsingError(self.parser.parse_aggregations, invalid)

    def test_parse_unknown_metric_op(self):
        invalid = {'a1': {'group': [{'distinct': {'field': 'http_code'}}],
                          'metric': 'unknown'}}
        self.assertParsingError(self.parser.parse_aggregations, invalid)

    def test_parse_missing_group(self):
        invalid = {'a1': {'metric': 'count'}}
        self.assertParsingError(self.parser.parse_aggregations, invalid)

    def test_parse_wrong_group_format(self):
        # group should be a list of group ops
        invalid = {'a1': {'group': {'unknown': {'field': 'http_code'}},
                          'metric': 'count'}}
        self.assertParsingError(self.parser.parse_aggregations, invalid)

    def test_wrong_agg_field(self):
        # `path` is not an aggregation field
        invalid = {'a1': {'group': ['path']}}
        parsed = self.parser.parse_aggregations(invalid)
        self.assertParsingError(parsed.validate)

    def test_wrong_distinct_field(self):
        # `metadata.title.nb` is not a categorical field
        invalid = {'a1': {'group': ['metadata.title.nb']}}
        parsed = self.parser.parse_aggregations(invalid)
        self.assertParsingError(parsed.validate)

    def test_wrong_range_field(self):
        # `host` is not a numerical field
        invalid = {'a1': {'group': [
            {'range': {'field': 'host', 'ranges': [{'to': 20}]}}]}}
        parsed = self.parser.parse_aggregations(invalid)
        self.assertParsingError(parsed.validate)

    def test_wrong_range_structure(self):
        # unknown range param
        invalid = {'a1': {'group': [{
            'range': {
                'field': 'http_code',
                'ranges': [{'a': 100, 'b': 200}]
            }}]}}
        parsed = self.parser.parse_aggregations(invalid)
        self.assertParsingError(parsed.validate)

        # too much param
        invalid = {'a1': {'group': [{
            'range': {
                'field': 'http_code',
                'ranges': [{'from': 100, 'to': 200, 'tooooo': 250}]
            }}]}}
        parsed = self.parser.parse_aggregations(invalid)
        self.assertParsingError(parsed.validate)
