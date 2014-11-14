import unittest
from cdf.query.sort import AscendingSort, DescendingSort


class TestAscendingSort(unittest.TestCase):
    def test_to_dict(self):
        expected_result = [{"foo": {"order": "asc"}}]
        self.assertEqual(expected_result, AscendingSort("foo").to_dict())


class TestDescendingSort(unittest.TestCase):
    def test_to_dict(self):
        expected_result = [{"foo": {"order": "desc"}}]
        self.assertEqual(expected_result, DescendingSort("foo").to_dict())
