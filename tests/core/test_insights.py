import unittest

from cdf.core.insights import Insight
from cdf.query.filter import EqFilter


class TestInsight(unittest.TestCase):
    def setUp(self):
        self.identifier = "foo"
        self.title = "Foo"
        self.eq_filter = EqFilter("bar", 5)
        self.aggs = [{"metrics": [{"max": "depth"}]}]

    def test_es_query_nominal_case(self):
        insight = Insight(self.identifier,
                          self.title,
                          self.eq_filter,
                          self.aggs)
        expected_es_query = {
            "filters": {
                "field": "bar",
                "predicate": "eq",
                "value": 5
            },
            "aggs": [
                {"metrics": [{"max": "depth"}]}
            ]
        }
        self.assertEqual(expected_es_query, insight.es_query)

    def test_es_query_default_aggregation(self):
        insight = Insight(self.identifier,
                          self.title,
                          self.eq_filter)
        expected_es_query = {
            "filters": {
                "field": "bar",
                "predicate": "eq",
                "value": 5
            },
            "aggs": [
                {"metrics": ["count"]}
            ]
        }
        self.assertEqual(expected_es_query, insight.es_query)

    def test_es_query_no_filter(self):
        insight = Insight(self.identifier,
                          self.title,
                          aggs=self.aggs)
        expected_es_query = {
            "aggs": [
                {"metrics": [{"max": "depth"}]}
            ]
        }
        self.assertEqual(expected_es_query, insight.es_query)

    def test_repr(self):
        insight = Insight(self.identifier, self.title)
        self.assertEqual(
            "foo: {'aggs': [{'metrics': ['count']}]}",
            repr(insight)
        )
