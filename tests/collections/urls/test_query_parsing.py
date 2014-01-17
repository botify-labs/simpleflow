import unittest
from cdf.collections.urls.query_parsing import parse_sorts, parse_fields, parse_predicate_filter, parse_not_filter, parse_boolean_filter
from cdf.exceptions import BotifyQueryException


class ParsingTestCase(unittest.TestCase):
    def assertParsingError(self, func, *args, **kwargs):
        self.assertRaises(BotifyQueryException,
                          func, *args, **kwargs)


class TestSortParsing(ParsingTestCase):
    def test_parsing(self):
        sort = ['field1', {'field3': {'order': 'desc'}}]
        result = parse_sorts(sort).transform()
        expected = ['field1', {'field3': {'order': 'desc', 'ignore_unmapped': True}}]
        self.assertEqual(result, expected)

    def test_wrong_sort_structure(self):
        # sorts should be a list
        invalid = {'sort': 'field'}
        self.assertParsingError(parse_sorts, invalid)

        # sort should not contain ints
        invalid = ['field', 'field2', 1]
        self.assertParsingError(parse_sorts, invalid)

        # order param should be `desc`
        # invalid = ['field', {'field2': {'order': 'hey!!'}}]
        # self.assertParsingError(parse_sorts, invalid)

        # order object should be in correct structure
        invalid = [{'field': 'desc'}]
        self.assertParsingError(parse_sorts, invalid)

        # order object should be in correct structure
        invalid = [{'field': {1, 2, 3}}]
        self.assertParsingError(parse_sorts, invalid)

        # sort element object should contain a single mapping
        invalid = [{'field': {'order': 'desc'}, 'field2': {'order': 'desc'}}]
        self.assertParsingError(parse_sorts, invalid)


class TestFieldsParsing(ParsingTestCase):
    def test_parsing(self):
        fields = ['field1', 'field2']
        result = parse_fields(fields).transform()
        expected = ['field1', 'field2']
        self.assertEqual(result, expected)

    def test_wrong_fields_structure(self):
        # fields should be a list of strings
        invalid = 1
        self.assertParsingError(parse_fields, invalid)

        # fields should be a list of strings
        invalid = [1, 'field1']
        self.assertParsingError(parse_fields, invalid)

        # fields should be a list of strings
        invalid = {'field0', 'field1'}
        self.assertParsingError(parse_fields, invalid)


class TestFilterParsing(ParsingTestCase):
    def test_parse_predicate_filter(self):
        valid = {'field': 'http_code', 'value': 200}
        expected = {'term': {'http_code': 200}}
        result = parse_predicate_filter(valid).transform()
        self.assertDictEqual(expected, result)

    def test_parse_not_filter(self):
        valid = {'not': {'field': 'http_code', 'value': 200}}
        expected = {'not': {'term': {'http_code': 200}}}
        result = parse_not_filter(valid).transform()
        self.assertDictEqual(expected, result)

    def test_parse_boolean_filter(self):
        valid = {'and': [{'field': 'http_code', 'value': 200}]}
        expected = {'and': [{'term': {'http_code': 200}}]}
        result = parse_boolean_filter(valid).transform()
        self.assertDictEqual(expected, result)

    def test_wrong_predicate_filter_semantic(self):
        # non-list predicate on list field
        invalid = {'predicate': 'eq', 'field': 'metadata.h1', 'value': 'data'}
        self.assertParsingError(parse_predicate_filter, invalid)

        # list predicate on non-list field
        invalid = {'predicate': 'any.contains', 'field': 'path', 'value': 'data'}
        self.assertParsingError(parse_predicate_filter, invalid)

        # value is missing
        invalid = {'predicate': 'eq', 'field': 'path'}
        self.assertParsingError(parse_predicate_filter, invalid)

        # value is not required
        invalid = {'predicate': 'not_null', 'field': 'path', 'value': 'data'}
        self.assertParsingError(parse_predicate_filter, invalid)