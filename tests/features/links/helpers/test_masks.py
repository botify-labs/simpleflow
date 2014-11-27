import unittest
from cdf.features.links.helpers.masks import get_key_to_nofollow_combination


class TestGetKeyToNofollowCombination(unittest.TestCase):
    def test_nominal_case(self):
        actual_result = get_key_to_nofollow_combination(
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
        actual_result = get_key_to_nofollow_combination(
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
        actual_result = get_key_to_nofollow_combination([], [])
        expected_result = {
            (): ""
        }
        self.assertEquals(expected_result, actual_result)
