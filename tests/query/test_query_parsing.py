import unittest
from cdf.query.query_parsing import (parse_sorts, parse_fields,
                                                parse_predicate_filter, parse_not_filter,
                                                parse_boolean_filter, parse_filter)
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

        sort = ['id', {'http_code': {'order': 'asc'}}]
        result = parse_sorts(sort).transform()
        expected = [{'id': {'ignore_unmapped': True}},
                    {'http_code': {'order': 'asc', 'ignore_unmapped': True}}]
        self.assertEqual(result, expected)

    def test_wrong_sort_structure(self):
        # sorts should be a list
        invalid = {'sort': 'field'}
        self.assertParsingError(parse_sorts, invalid)

        # sort should not contain ints
        invalid = ['url', 'path', 1]
        self.assertParsingError(parse_sorts, invalid)

        # order object should be in correct structure
        invalid = [{'url': 'desc'}]
        self.assertParsingError(parse_sorts, invalid)

        # order object should be in correct structure
        invalid = [{'url': {1, 2, 3}}]
        self.assertParsingError(parse_sorts, invalid)

        # sort element object should contain a single mapping
        invalid = [{'url': {'order': 'desc'}, 'http_code': {'order': 'desc'}}]
        self.assertParsingError(parse_sorts, invalid)

    def test_wrong_sort_semantic(self):
        # sort field is not a child field
        # `error_links.3xx` is a valid query field but not a target for sort
        invalid = [{'error_links.3xx': {'order': 'desc'}}]
        parsed = parse_sorts(invalid)
        self.assertParsingError(parsed.validate)

        # order param should be `desc`
        invalid = ['url', {'path': {'order': 'hey!!'}}]
        parsed = parse_sorts(invalid)
        self.assertParsingError(parsed.validate)


class TestFieldsParsing(ParsingTestCase):
    def test_parsing(self):
        fields = ['url', 'content_type']
        result = parse_fields(fields).transform()
        expected = ['url', 'content_type']
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
        parsed = parse_fields(invalid)
        self.assertParsingError(parsed.validate)


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
        # boolean filter should contain a list of other filters
        invalid = {'or': {'and': [{'field': 'http_code', 'value': 200}]}}
        self.assertParsingError(parse_filter, invalid)

        valid = {'and': [{'field': 'http_code', 'value': 200}]}
        expected = {'and': [{'term': {'http_code': 200}}]}
        result = parse_boolean_filter(valid).transform()
        self.assertDictEqual(expected, result)

    def test_wrong_predicate_filter_semantic(self):
        # non-list predicate on list field
        invalid = {'predicate': 'eq', 'field': 'metadata.h1', 'value': 'data'}
        parsed = parse_predicate_filter(invalid)
        self.assertParsingError(parsed.validate)

        # list predicate on non-list field
        invalid = {'predicate': 'any.contains', 'field': 'path', 'value': 'data'}
        parsed = parse_predicate_filter(invalid)
        self.assertParsingError(parsed.validate)

        # value is missing
        invalid = {'predicate': 'eq', 'field': 'path'}
        parsed = parse_predicate_filter(invalid)
        self.assertParsingError(parsed.validate)

        # value is not required
        invalid = {'predicate': 'exists', 'field': 'path', 'value': 'data'}
        parsed = parse_predicate_filter(invalid)
        self.assertParsingError(parsed.validate)

        # value is of wrong type
        invalid = {'predicate': 'eq', 'field': 'path', 'value': ['data']}
        parsed = parse_predicate_filter(invalid)
        self.assertParsingError(parsed.validate)

        # predicate field is not a child field
        # `error_links` is a valid query field, but not valid as a target
        # of predicate
        invalid = {'predicate': 'exists', 'field': 'error_links'}
        parsed = parse_predicate_filter(invalid)
        self.assertParsingError(parsed.validate)