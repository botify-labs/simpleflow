import unittest
from cdf.query.sort import AscendingSort, DescendingSort


class TestAscendingSort(unittest.TestCase):
    def test_to_dict(self):
        expected_result = {"asc": "foo"}
        self.assertEqual(expected_result, AscendingSort("foo").to_dict())


class TestDescendingSort(unittest.TestCase):
    def test_to_dict(self):
        expected_result = {"desc": "foo"}
        self.assertEqual(expected_result, DescendingSort("foo").to_dict())
