import unittest

from cdf.query.aggregation import (MetricAggregation,
                                   AvgAggregation,
                                   MinAggregation,
                                   MaxAggregation,
                                   CountAggregation)


class TestMetricAggregation(unittest.TestCase):
    def setUp(self):
        self.field = "foo"

    def test_metric_aggregation(self):
        self.assertEqual(
            {"op": self.field},
            MetricAggregation("op", self.field).to_dict()
        )

    def test_avg_aggregation(self):
        self.assertEqual(
            {"avg": self.field},
            AvgAggregation(self.field).to_dict()
        )

    def test_min_aggregation(self):
        self.assertEqual(
            {"min": self.field},
            MinAggregation(self.field).to_dict()
        )

    def test_max_aggregation(self):
        self.assertEqual(
            {"max": self.field},
            MaxAggregation(self.field).to_dict()
        )

    def test_count_aggregation(self):
        self.assertEqual(
            {"count": self.field},
            CountAggregation(self.field).to_dict()
        )

