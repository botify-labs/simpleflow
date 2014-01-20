import unittest
from cdf.collections.urls.query_parsing import (parse_sorts, parse_fields,
                                                parse_predicate_filter, parse_not_filter,
                                                parse_boolean_filter)
from cdf.exceptions import BotifyQueryException


class ParsingTestCase(unittest.TestCase):
    def assertParsingError(self, func, *args, **kwargs):
        self.assertRaises(BotifyQueryException,
                          func, *args, **kwargs)


class TestSortParsing(ParsingTestCase):
    def test_parsing(self):
        sort = ['id', {'http_code': {'order': 'desc'}}]
        result = parse_sorts(sort).transform()
        expected = [{'id': {'ignore_unmapped': True}},
                    {'http_code': {'order': 'desc', 'ignore_unmapped': True}}]
        self.assertEqual(result, expected)

    def test_wrong_sort_structure(self):
        # sorts should be a list
        invalid = {'sort': 'field'}
        self.assertParsingError(parse_sorts, invalid)

        # sort should not contain ints
        invalid = ['field', 'field2', 1]
        self.assertParsingError(parse_sorts, invalid)

        # order param should be `desc`
        invalid = ['field', {'field2': {'order': 'hey!!'}}]
        self.assertParsingError(parse_sorts, invalid)

        # order object should be in correct structure
        invalid = [{'field': 'desc'}]
        self.assertParsingError(parse_sorts, invalid)

        # order object should be in correct structure
        invalid = [{'field': {1, 2, 3}}]
        self.assertParsingError(parse_sorts, invalid)

        # sort element object should contain a single mapping
        invalid = [{'field': {'order': 'desc'}, 'field2': {'order': 'desc'}}]
        self.assertParsingError(parse_sorts, invalid)

    def test_wrong_sort_semantic(self):
        # sort field is not a child field
        # `error_links.3xx` is a valid query field but not a target for sort
        invalid = [{'error_links.3xx': {'order': 'desc'}}]
        self.assertParsingError(parse_sorts, invalid)


class TestFieldsParsing(ParsingTestCase):
    def test_parsing(self):
        fields = ['url', 'path']
        result = parse_fields(fields).transform()
        expected = ['url', 'path']
        self.assertEqual(result, expected)

    def test_wrong_fields_structure(self):
        # fields should be a list of strings
        invalid = 1
        self.assertParsingError(parse_fields, invalid)

        # fields should be a list of strings
        invalid = [1, 'url']
        self.assertParsingError(parse_fields, invalid)

        # fields should be a list of strings
        invalid = {'url', 'path'}
        self.assertParsingError(parse_fields, invalid)

    def test_wrong_fields_semantic(self):
        # field should be a valid one
        invalid = ['field0', 'path']
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

        # predicate field is not a child field
        # `error_links` is a valid query field, but not valid as a target
        # of predicate
        invalid = {'predicate': 'not_null', 'field': 'error_links'}
        self.assertParsingError(parse_predicate_filter, invalid)