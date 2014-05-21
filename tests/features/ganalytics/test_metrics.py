import unittest
from cdf.features.ganalytics.metrics import (compute_average_value,
                                             compute_percentage)


class TestComputeAverageValue(unittest.TestCase):
    def test_nominal_case(self):
        self.assertEqual(16.67, compute_average_value(50, 3))

    def test_null_sessions(self):
        self.assertEqual(0, compute_average_value(2, 0))


class TestComputePercentage(unittest.TestCase):
    def test_nominal_case(self):
        self.assertEqual(66.67, compute_percentage(2, 3))

    def test_null_sessions(self):
        self.assertEqual(0, compute_percentage(2, 0))
