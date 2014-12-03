import unittest
from cdf.utils.list import pad_list


class TestPadList(unittest.TestCase):
    def test_nominal_case(self):
        goal_length = 5
        fill_value = 0
        self.assertEqual(
            [1, 2, 0, 0, 0],
            pad_list([1, 2], goal_length, fill_value)
        )

    def test_long_list(self):
        goal_length = 2
        fill_value = 0
        self.assertEqual(
            [1, 2, 3, 4],
            pad_list([1, 2, 3, 4], goal_length, fill_value)
        )

    def test_exact_length(self):
        goal_length = 4
        fill_value = 0
        self.assertEqual(
            [1, 2, 3, 4],
            pad_list([1, 2, 3, 4], goal_length, fill_value)
        )
