import unittest

from cdf.core.insights import Insight
from cdf.query.filter import EqFilter
from cdf.query.aggregation import MaxAggregation


class TestInsight(unittest.TestCase):
    def setUp(self):
        self.identifier = "foo"
        self.title = "Foo"
        self.eq_filter = EqFilter("bar", 5)
        self.max_agg = MaxAggregation("depth")

    def test_query_nominal_case(self):
        insight = Insight(self.identifier,
                          self.title,
                          self.eq_filter,
                          self.max_agg)
        expected_query = {
            "filters": {
                "field": "bar",
                "predicate": "eq",
                "value": 5
            },
            "aggs": [
                {"metrics": [{"max": "depth"}]}
            ]
        }
        self.assertEqual(expected_query, insight.query)

    def test_query_default_aggregation(self):
        insight = Insight(self.identifier,
                          self.title,
                          self.eq_filter)
        expected_query = {
            "filters": {
                "field": "bar",
                "predicate": "eq",
                "value": 5
            },
            "aggs": [
                {"metrics": [{"count": "url"}]}
            ]
        }
        self.assertEqual(expected_query, insight.query)

    def test_query_no_filter(self):
        insight = Insight(self.identifier,
                          self.title,
                          metric_agg=self.max_agg)
        expected_query = {
            "aggs": [
                {"metrics": [{"max": "depth"}]}
            ]
        }
        self.assertEqual(expected_query, insight.query)

    def test_repr(self):
        insight = Insight(self.identifier, self.title)
        self.assertEqual(
            "foo: {'aggs': [{'metrics': [{'count': 'url'}]}]}",
            repr(insight)
        )
