import unittest

from valideer import ValidationError

from cdf.collections.urls.query_validator import *


class TestQueryValidation(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def assertRaisesWithMessage(self, msg, exc_class,
                                func, *args, **kwargs):
        try:
            func(*args, **kwargs)
            self.fail(msg='Exception expected')
        except Exception as inst:
            self.assertTrue(isinstance(inst, exc_class))
            self.assertEqual(inst.message, msg)

    def assertValidationErrorWithMessage(self, msg,
                                         func, *args, **kwargs):
        self.assertRaisesWithMessage(msg, ValidationError,
                                     func, *args, **kwargs)

    def assertValidationError(self, func, *args, **kwargs):
        self.assertRaises(ValidationError,
                          func, *args, **kwargs)

    def test_validate_sorts(self):
        # sorts should be a list
        invalid = {'sort': 'field'}
        self.assertValidationError(validate_sorts, invalid)

        # sort should not contain ints
        invalid = ['field', 'field2', 1]
        self.assertValidationError(validate_sorts, invalid)

        # order param should be `desc`
        invalid = ['field', {'field2': {'order': 'hey!!'}}]
        self.assertValidationError(validate_sorts, invalid)

        # order object should be in correct structure
        invalid = [{'field': 'desc'}]
        self.assertValidationError(validate_sorts, invalid)

        # order object should be in correct structure
        invalid = [{'field': {1, 2, 3}}]
        self.assertValidationError(validate_sorts, invalid)

        # sort element object should contain a single mapping
        invalid = [{'field': {'order': 'desc'}, 'field2': {'order': 'desc'}}]
        self.assertValidationError(validate_sorts, invalid)

        # a valid one
        valid = ['field1', {'field2': {'order': 'desc'}}, 'field3']
        validate_sorts(valid)

    def test_validate_bool_filter(self):
        # bool filter should be a mapping
        invalid = 1
        self.assertValidationError(validate_boolean_filter, invalid)

        # bool predicate should be one of `and`, `or` or `not`
        invalid = {'abc': []}
        self.assertValidationError(validate_boolean_filter, invalid)

        # contained filters should be in a list
        invalid = {'and': {'filters': []}}
        self.assertValidationError(validate_boolean_filter, invalid)

        # bool filter should be a single mapping
        invalid = {'and': [], 'or': []}
        self.assertValidationError(validate_boolean_filter, invalid)

        # a valid one
        valid = {'and': ['something', 'not important']}
        validate_boolean_filter(valid)

    def test_validate_fields(self):
        # fields should be a list of strings
        invalid = 1
        self.assertValidationError(validate_fields, invalid)

        # fields should be a list of strings
        invalid = [1, 'field1']
        self.assertValidationError(validate_fields, invalid)

        # fields should be a list of strings
        invalid = {'field0', 'field1'}
        self.assertValidationError(validate_fields, invalid)

        # a valid one
        valid = ['field0', 'field1']
        validate_fields(valid)

    def test_validate_predicate_filter(self):
        invalid = 1
        self.assertValidationError(validate_predicate_filter, invalid)

        invalid = {'predicate': 'any.eq', 'field': 'metadata.h1'}
        self.assertValidationError(validate_predicate_filter, invalid)

        invalid = {'predicate': 'eq', 'field': 'metadata.h1', 'value': 'data'}
        self.assertValidationErrorWithMessage('Apply non-list predicate on list field',
                                              validate_predicate_filter, invalid)

        invalid = {'predicate': 'any.contains', 'field': 'path', 'value': 'data'}
        self.assertValidationErrorWithMessage('Apply list predicate on non-list field',
                                              validate_predicate_filter, invalid)

        # valid predicate filter, should not throw anything
        valid = {'field': 'hash', 'value': 123456789098761111}
        validate_predicate_filter(valid)