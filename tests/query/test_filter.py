import unittest
from cdf.query.filter import (FilterCombination,
                              AndFilter,
                              OrFilter,
                              NotFilter,
                              ConcreteFilter,
                              EqFilter,
                              LtFilter,
                              LteFilter,
                              GtFilter,
                              GteFilter,
                              BetweenFilter)


class TestFilterCombinations(unittest.TestCase):
    def setUp(self):
        self.eq_filter = EqFilter("foo", 2)
        self.gte_filter = GteFilter("bar", 5)

    def test_filter_combination(self):
        self.assertEqual(
            {"foo": [self.eq_filter.to_dict(), self.gte_filter.to_dict()]},
            FilterCombination("foo", [self.eq_filter, self.gte_filter]).to_dict()
        )

    def test_and_filter(self):
        self.assertEqual(
            {"and": [self.eq_filter.to_dict(), self.gte_filter.to_dict()]},
            AndFilter([self.eq_filter, self.gte_filter]).to_dict()
        )

    def test_or_filter(self):
        self.assertEqual(
            {"or": [self.eq_filter.to_dict(), self.gte_filter.to_dict()]},
            OrFilter([self.eq_filter, self.gte_filter]).to_dict()
        )


class TestNotFilter(unittest.TestCase):
    def test_nominal_case(self):
        eq_filter = EqFilter("foo", 2)
        self.assertEqual(
            {"not": eq_filter.to_dict()},
            NotFilter(eq_filter).to_dict()
        )


class TestConcreteFilters(unittest.TestCase):
    def setUp(self):
        self.field = "foo"
        self.value = 2

    def test_concrete_filter(self):
        predicate = "bar"
        self.assertEqual(
            {"field": self.field, "predicate": predicate, "value": self.value},
            ConcreteFilter(self.field, predicate, self.value).to_dict()
        )

    def test_eq_filter(self):
        self.assertEqual(
            {"field": self.field, "predicate": "eq", "value": self.value},
            EqFilter(self.field, self.value).to_dict()
        )

    def test_lt_filter(self):
        self.assertEqual(
            {"field": self.field, "predicate": "lt", "value": self.value},
            LtFilter(self.field, self.value).to_dict()
        )

    def test_lte_filter(self):
        self.assertEqual(
            {"field": self.field, "predicate": "lte", "value": self.value},
            LteFilter(self.field, self.value).to_dict()
        )

    def test_gt_filter(self):
        self.assertEqual(
            {"field": self.field, "predicate": "gt", "value": self.value},
            GtFilter(self.field, self.value).to_dict()
        )

    def test_gte_filter(self):
        self.assertEqual(
            {"field": self.field, "predicate": "gte", "value": self.value},
            GteFilter(self.field, self.value).to_dict()
        )

    def test_between_filter(self):
        crt_range = [2, 3]
        self.assertEqual(
            {"field": self.field, "predicate": "between", "value": crt_range},
            BetweenFilter(self.field, crt_range).to_dict()
        )
