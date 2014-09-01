import unittest
import mock

from cdf.core.features import Feature
from cdf.core.insights import Insight, InsightTrendPoint, InsightValue
from cdf.query.filter import EqFilter
from cdf.query.query import Query
from cdf.tasks.insights import (get_query_agg_result,
                                get_features,
                                compute_insight_value,
                                compute_insight_values)


class TestGetQueryAggResult(unittest.TestCase):
    def test_nominal_case(self):
        query = mock.create_autospec(Query)
        query.aggs = [{"metrics": [5]}]
        self.assertEqual(5, get_query_agg_result(query))

    def test_no_result_case(self):
        query = mock.create_autospec(Query)
        query.aggs = []
        self.assertEqual(0, get_query_agg_result(query))


class TestComputeInsightValue(unittest.TestCase):
    @mock.patch("cdf.tasks.insights.get_query_agg_result", autospec=True)
    @mock.patch("cdf.tasks.insights.Query", autospec=True)
    def test_nominal_case(self,
                          query_mock,
                          get_query_agg_result_mock):
        #mocking
        get_query_agg_result_mock.return_value = 3.14

        #definition
        insight = Insight(
            "foo",
            "Foo insight",
            EqFilter("foo_field", 1001)
        )
        feature_name = "feature"
        crawls = [1001, 2008]
        es_location = "http://elasticsearch.com"
        es_index = "es_index"

        #actual call
        actual_result = compute_insight_value(insight,
                                              feature_name,
                                              crawls,
                                              es_location,
                                              es_index)

        #check values
        expected_trend = [
            InsightTrendPoint(1001, 3.14),
            InsightTrendPoint(2008, 3.14)
        ]
        expected_result = InsightValue(insight, feature_name, expected_trend)

        self.assertEqual(expected_result.to_dict().values(),
                         actual_result.to_dict().values())
        self.assertEqual(expected_result.to_dict(), actual_result.to_dict())

        #check the calls to Query.__init__()
        expected_query_calls = [
            mock.call(es_location, es_index, "urls", 1001, 0, insight.query),
            mock.call(es_location, es_index, "urls", 2008, 0, insight.query),
        ]
        self.assertEqual(expected_query_calls, query_mock.mock_calls)


@mock.patch("cdf.tasks.insights.Feature.get_features")
class TestGetFeatures(unittest.TestCase):
    def setUp(self):
        self.features = [
            Feature("foo", "Foo", "this is foo", None),
            Feature("bar", "Bar", "this is bar", None),
            Feature("baz", "Baz", "this is baz", None)
        ]

    def test_nominal_case(self, get_features_mock):
        get_features_mock.return_value = self.features
        actual_result = get_features(["foo", "baz"])
        self.assertEqual(["foo", "baz"],
                         [feature.identifier for feature in actual_result])

    def test_unexisting_feature(self, get_features_mock):
        get_features_mock.return_value = self.features
        self.assertRaises(ValueError,
                          get_features,
                          ["qux"])


class TestComputeInsightValues(unittest.TestCase):
    @mock.patch("cdf.tasks.insights.compute_insight_value", autospec=True)
    def test_nominal_case(self, compute_insight_value_mock):

        #we don't really care about the result
        compute_insight_value_mock.return_value = InsightValue(None, "", [])

        insight1 = Insight("1", "Insight 1", EqFilter("field", 1))
        insight2 = Insight("2", "Insight 2", EqFilter("field", 2))
        insight3 = Insight("3", "Insight 3", EqFilter("field", 3))

        feature1 = mock.create_autospec(Feature)
        feature1.name = "feature1"
        feature1.get_insights.return_value = [insight1]

        feature2 = mock.create_autospec(Feature)
        feature2.name = "feature2"
        feature2.get_insights.return_value = [insight2, insight3]

        crawls = [(1001, "13-08-2014")]
        features = [feature1, feature2]
        es_location = "http://elasticsearch.com"
        es_index = "botify"

        actual_result = compute_insight_values(crawls,
                                               features,
                                               es_location,
                                               es_index)
        #check results
        self.assertEqual(3, len(actual_result))
        self.assertTrue(
            all([isinstance(r, InsightValue) for r in actual_result])
        )
        expected_calls = [
            mock.call(insight1, 'feature1', crawls, es_location, es_index),
            mock.call(insight2, 'feature2', crawls, es_location, es_index),
            mock.call(insight3, 'feature2', crawls, es_location, es_index),
        ]

        self.assertEqual(expected_calls, compute_insight_value_mock.mock_calls)
