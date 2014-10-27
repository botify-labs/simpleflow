import unittest
from cdf.features.comparison.diff import (
    compute_qualitative_diff,
    compute_quantitative_diff,
    document_diff,
    diff, compute_qualitative_diff_list)
from cdf.features.comparison.constants import (
    QualitativeDiffResult as qdr,
    MatchingState
)


class TestDocumentDiff(unittest.TestCase):
    TEST_DIFF_STRATEGY = {
        'a.quantitative': compute_quantitative_diff,
        'b.qualitative': compute_qualitative_diff,
        'c.list': compute_qualitative_diff_list
    }

    def test_qualitative_field_changed(self):
        ref_doc = {'b': {'qualitative': 123}}
        new_doc = {'b': {'qualitative': 'url_foo'}}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)

        expected = {'b': {'qualitative': qdr.CHANGED}}
        self.assertEqual(diff_result, expected)

    def test_qualitative_field_equal(self):
        ref_doc = {'b': {'qualitative': 123}}
        new_doc = {'b': {'qualitative': 123}}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)

        expected = {'b': {'qualitative': qdr.EQUAL}}
        self.assertEqual(diff_result, expected)

    def test_qualitative_field_appeared(self):
        ref_doc = {}
        new_doc = {'b': {'qualitative': 200}}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)
        expected = {'b': {'qualitative': qdr.APPEARED}}

        self.assertEqual(diff_result, expected)

    def test_qualitative_field_disappeared(self):
        ref_doc = {'b': {'qualitative': 200}}
        new_doc = {}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)
        expected = {'b': {'qualitative': qdr.DISAPPEARED}}

        self.assertEqual(diff_result, expected)

    def test_quantitative_field(self):
        ref_doc = {'a': {'quantitative': 123}}
        new_doc = {'a': {'quantitative': 125}}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)

        expected = {'a': {'quantitative': -2}}
        self.assertEqual(diff_result, expected)

    def test_quantitative_field_missing(self):
        ref_doc = {}
        new_doc = {'a': {'quantitative': 125}}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)

        self.assertEqual(diff_result, None)

    def test_qualitative_field_both_missing(self):
        diff_result = document_diff(
            {}, {}, diff_strategy=self.TEST_DIFF_STRATEGY)
        self.assertEqual(diff_result, None)

    def test_list_field_both_empty(self):
        ref_doc = {'c': {'list': []}}
        new_doc = {'c': {'list': []}}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)
        self.assertEqual(diff_result, {'c': {'list': qdr.EQUAL}})

    def test_list_field_one_empty(self):
        ref_doc = {'c': {'list': ['a']}}
        new_doc = {'c': {'list': []}}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)
        self.assertEqual(diff_result, {'c': {'list': qdr.DISAPPEARED}})

        ref_doc = {'c': {'list': []}}
        new_doc = {'c': {'list': ['b']}}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)
        self.assertEqual(diff_result, {'c': {'list': qdr.APPEARED}})

    def test_list_field_compare(self):
        # only first element of the list is taken into account
        ref_doc = {'c': {'list': ['a']}}
        new_doc = {'c': {'list': ['b']}}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)
        self.assertEqual(diff_result, {'c': {'list': qdr.CHANGED}})

        ref_doc = {'c': {'list': ['a', 'b']}}
        new_doc = {'c': {'list': ['a']}}

        diff_result = document_diff(
            ref_doc, new_doc, diff_strategy=self.TEST_DIFF_STRATEGY)
        self.assertEqual(diff_result, {'c': {'list': qdr.EQUAL}})

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
                                   {'b': {'qualitative': qdr.CHANGED}})),
            (MatchingState.DISAPPEAR, ({}, None, None)),
        ]
        self.assertEqual(results, expected)