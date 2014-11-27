import unittest
from cdf.features.links.helpers.masks import (
    build_nofollow_combination_lookup, compute_nofollow_combination
)

class TestComputeNoFollowCombination(unittest.TestCase):
    def test_nominal_case(self):
        self.assertEqual(
            "meta_robots",
            compute_nofollow_combination(["robots", "meta"], ["robots", "meta"])
        )

    def test_allowed_masks(self):
        self.assertEqual(
            "meta_robots",
            compute_nofollow_combination(["robots", "meta", "prev"], ["robots", "meta"])
        )

class TestBuildNofollowCombinationLookup(unittest.TestCase):
    def test_nominal_case(self):
        actual_result = build_nofollow_combination_lookup(
            ["foo", "bar"], ["foo", "bar"]
        )
        expected_result = {
            (): "",
            ("foo",): "foo",
            ("bar",): "bar",
            ("foo", "bar"): "bar_foo",
            ("bar", "foo"): "bar_foo"
        }
        self.assertEquals(expected_result, actual_result)

    def test_allowed_masks(self):
        actual_result = build_nofollow_combination_lookup(
            ["foo", "bar", "baz"],
            ["foo", "bar"]
        )
        expected_result = {
            (): "",
            ("foo",): "foo",
            ("bar",): "bar",
            ("baz",): "",
            ("foo", "bar"): "bar_foo",
            ("bar", "foo"): "bar_foo",
            ("foo", "baz"): "foo",
            ("baz", "foo"): "foo",
            ("bar", "baz"): "bar",
            ("baz", "bar"): "bar",
            ("foo", "bar", "baz"): "bar_foo",
            ("foo", "baz", "bar"): "bar_foo",
            ("bar", "foo", "baz"): "bar_foo",
            ("bar", "baz", "foo"): "bar_foo",
            ("baz", "foo", "bar"): "bar_foo",
            ("baz", "bar", "foo"): "bar_foo"
        }
        self.assertEquals(expected_result, actual_result)

    def test_empty_mask_ids(self):
        actual_result = build_nofollow_combination_lookup([], [])
        expected_result = {
            (): ""
        }
        self.assertEquals(expected_result, actual_result)
