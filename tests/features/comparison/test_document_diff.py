import unittest
from cdf.features.comparison.diff import (
    qualitative_diff,
    quantitative_diff,
    document_diff,
    diff)
from cdf.features.comparison.constants import (
    CHANGED,
    EQUAL,
    APPEARED,
    DISAPPEARED,
    MatchingState)


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

    def test_diff_document_stream(self):
        doc_a = {'a': {'quantitative': 123}}
        doc_b_ref = {'b': {'qualitative': 'abc'}}
        doc_b_new = {'b': {'qualitative': 'def'}}
        # elem 2 and 3 are diff-able
        # first and last are not diff-able, should be ignored
        matched_stream = iter([
            (MatchingState.DISCOVER, (None, {})),
            (MatchingState.MATCH, ({}, doc_a)),
            (MatchingState.MATCH, (doc_b_ref, doc_b_new)),
            (MatchingState.DISAPPEAR, ({}, None)),
        ])

        results = list(diff(matched_stream,
                            diff_strategy=self.TEST_DIFF_STRATEGY))
        expected = [
            (MatchingState.DISCOVER, (None, {}, None)),
            (MatchingState.MATCH, ({}, doc_a, None)),
            (MatchingState.MATCH, (doc_b_ref, doc_b_new,
                                   {'b': {'qualitative': CHANGED}})),
            (MatchingState.DISAPPEAR, ({}, None, None)),
        ]
        self.assertEqual(results, expected)

