import unittest
from cdf.features.comparison.diff import (
    qualitative_diff,
    quantitative_diff,
    document_diff
)
from cdf.features.comparison.constants import (
    CHANGED,
    EQUAL,
    APPEARED,
    DISAPPEARED
)


class TestDocumentDiff(unittest.TestCase):
    TEST_DIFF_STRATEGY = {
        'a.quantitative': quantitative_diff,
        'b.qualitative': qualitative_diff
    }

    def test_qualitative_field_changed(self):
        ref_doc = {'b': {'qualitative': 123}}
        new_doc = {'b': {'qualitative': 'url_foo'}}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)

        expected = {'b': {'qualitative': CHANGED}}
        self.assertEqual(diff_result, expected)

    def test_qualitative_field_equal(self):
        ref_doc = {'b': {'qualitative': 123}}
        new_doc = {'b': {'qualitative': 123}}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)

        expected = {'b': {'qualitative': EQUAL}}
        self.assertEqual(diff_result, expected)

    def test_qualitative_field_appeared(self):
        ref_doc = {}
        new_doc = {'b': {'qualitative': 200}}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)
        expected = {'b': {'qualitative': APPEARED}}

        self.assertEqual(diff_result, expected)

    def test_qualitative_field_disappeared(self):
        ref_doc = {'b': {'qualitative': 200}}
        new_doc = {}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)
        expected = {'b': {'qualitative': DISAPPEARED}}

        self.assertEqual(diff_result, expected)

    def test_quantitative_field(self):
        ref_doc = {'a': {'quantitative': 123}}
        new_doc = {'a': {'quantitative': 125}}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)

        expected = {'a': {'quantitative': -2}}
        self.assertEqual(diff_result, expected)
