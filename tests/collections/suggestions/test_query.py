import unittest
import mock

import pandas as pd

from cdf.collections.suggestions.query import SuggestQuery


class TestSuggestQuery(unittest.TestCase):
    def setUp(self):
        #This is not a real hdfstore object
        #because instanciating a HdfStore would require to create
        #a temporary file
        #but a HdfStore object behaves essentially like a dict of DataFrame
        hdfstore = {}

        hashes = ["1", "2", "3"]
        requests = {
            "string": ["string1", "string2", "string3"],
            "verbose_string": ['{"query": "v_string1"}',
                               '{"query": "v_string2"}',
                               '{"query": "v_string3"}']
            }
        hdfstore["requests"] = pd.DataFrame(requests, index=hashes)

        #all patterns are children of pattern3
        children_relationship = {
            "parent": ["3", "3"],
            "child": ["1", "2"]
            }
        hdfstore["children"] = pd.DataFrame(children_relationship)

        hdfstore["suggest"] = pd.DataFrame()
        self.suggest_query = SuggestQuery(hdfstore)

    def test_query_hash_to_string(self):
        self.assertEqual("string3", self.suggest_query.query_hash_to_string(3))

    def test_query_hash_to_string_unexisting_hash(self):
        self.assertRaises(
            KeyError,
            self.suggest_query.query_hash_to_string,
            "does_not_exist")

    def test_query_hash_to_verbose_string(self):
        self.assertEqual({"query": "v_string1"},
                         self.suggest_query.query_hash_to_verbose_string(1))

    def test_query_hash_to_verbose_string_unexisting_hash(self):
        self.assertRaises(
            KeyError,
            self.suggest_query.query_hash_to_verbose_string,
            "does_not_exist")

    def test_is_child(self):
        self.assertTrue(self.suggest_query.is_child("3", "1"))
        self.assertFalse(self.suggest_query.is_child("2", "1"))

    def test_is_child_unexisting_hash(self):
        self.assertFalse(self.suggest_query.is_child("does_not_exist", "1"))

    def test_compute_child_relationship_set(self):
        child_relationship = {
            "parent": ["1", "1", "3"],
            "child":  ["2", "3", "4"]
        }
        frame = pd.DataFrame(child_relationship)
        expected_result = set([("1", "2"), ("1", "3"), ("3", "4")])
        self.assertSetEqual(expected_result,
                            self.suggest_query.compute_child_relationship_set(frame))

    def test_sort_results_by_target_field_count(self):
        query = {"target_field": "field1"}

        results = [
            {"query": "string1", "counters": {"field1": 2, "pages_nb": 4}},
            {"query": "string2", "counters": {"field1": 1, "pages_nb": 5}}
            ]

        #default ordering is descending
        expected_result = [
            {"query": "string1", "counters": {"field1": 2, "pages_nb": 4}},
            {"query": "string2", "counters": {"field1": 1, "pages_nb": 5}}
            ]

        self.assertListEqual(expected_result,
                             self.suggest_query.sort_results_by_target_field_count(query, results))

        #if target_field is not set use "pages_nb"
        query = {}
        expected_result.reverse()
        self.assertListEqual(expected_result,
                             self.suggest_query.sort_results_by_target_field_count(query, results))

        #change ordering
        query = {"target_sort": "asc"}
        expected_result.reverse()
        self.assertListEqual(expected_result,
                             self.suggest_query.sort_results_by_target_field_count(query, results))

    def test_remove_equivalent_parents(self):

        settings = {"target_field": "field"}
        results = [
            {"query": "1", "counters": {"pages_nb": 1, "field": 10}},
            {"query": "2", "counters": {"pages_nb": 1, "field":  5}},
            {"query": "3", "counters": {"pages_nb": 2, "field": 10}}
        ]

        expected_result = [
            {"query": "1", "counters": {"pages_nb": 1, "field": 10}},
            {"query": "2", "counters": {"pages_nb": 1, "field": 5}}
        ]

        self.assertListEqual(expected_result,
                             self.suggest_query.remove_equivalent_parents(settings, results))

        #if target_filed is not set use "pages_nb" as target field
        #if the target field is "pages_nb",
        #the algorithm should not remove any element
        self.assertListEqual(results,
                             self.suggest_query.remove_equivalent_parents({}, results))

    def test_hide_less_relevant_children(self):
        results = [
            {"query": "1", "counters": {"pages_nb": 5}},
            {"query": "3", "counters": {"pages_nb": 2}},
            {"query": "2", "counters": {"pages_nb": 1}}
        ]

        #result "2" is removed from the result list since it
        expected_result = [
            {"query": "1", "counters": {"pages_nb": 5}},
            {"query": "3", "counters": {"pages_nb": 2}, "children": [{"query": "2", "counters": {"pages_nb": 1}}]}
        ]

        self.assertListEqual(expected_result,
                             self.suggest_query.hide_less_relevant_children(results))

    def test_resolve_result(self):
        resolve_verbose = False
        result = {"query": "1", "counters": {"pages_nb": 5}}

        expected_result = {
            "query_hash_id": 1,
            "query": u"string1",
            "query_bql": u"string1",
            "counters": {"pages_nb": 5}
        }
        self.suggest_query._resolve_result(result, resolve_verbose)
        self.assertDictEqual(expected_result, result)

    def test_resolve_result_verbose(self):
        resolve_verbose = True
        result = {"query": "1", "counters": {"pages_nb": 5}}

        expected_result = {
            "query_hash_id": 1,
            "query": u"string1",
            "query_bql": u"string1",
            "query_verbose": {"query": "v_string1"},
            "counters": {"pages_nb": 5}
        }
        self.suggest_query._resolve_result(result, resolve_verbose)
        self.assertDictEqual(expected_result, result)

    def test_resolve_results(self):
        results = [
            {"query": "1", "counters": {"pages_nb": 5}},
            {"query": "3", "counters": {"pages_nb": 2}, "children": [{"query": "2", "counters": {"pages_nb": 1}}]},
        ]

        expected_result = [
            {
                "query_hash_id": 1,
                "query": u"string1",
                "query_bql": u"string1",
                "counters": {"pages_nb": 5}
            },
            {
                "query_hash_id": 3,
                "query": u"string3",
                "query_bql": u"string3",
                "counters": {"pages_nb": 2},
                "children": [{"query_hash_id": 2,
                              "query": u"string2",
                              "query_bql": u"string2",
                              "query_verbose": {"query": "v_string2"},
                              "counters": {"pages_nb": 1}}
                             ]
            }]

        display_children = True
        self.assertListEqual(expected_result,
                             self.suggest_query._resolve_results(results, display_children))

        #do not display children

        #we redefine results as the previous call has modified it.
        results = [
            {"query": "1", "counters": {"pages_nb": 5}},
            {"query": "3", "counters": {"pages_nb": 2}, "children": [{"query": "2", "counters": {"pages_nb": 1}}]},
        ]
        display_children = False
        #the expected result is the same except
        #that the children entry is removed
        del expected_result[1]["children"]
        self.assertListEqual(expected_result,
                             self.suggest_query._resolve_results(results, display_children))
