# -*- coding:utf-8 -*-
import unittest
from cdf.analysis.urls.utils import merge_queries_filters


class TestUtils(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_merge_queries(self):
        field_1 = {"field": "http_code", "value": 200}
        field_2 = {"field": "depth", "value": 1}

        _or = {
            "or": [
                field_1,
                field_2
            ]
        }

        _and = {
            "and": [
                field_1,
                field_2
            ]
        }

        self.assertEquals(merge_queries_filters(field_1, field_2), {"and": [field_1, field_2]})
        self.assertEquals(merge_queries_filters(field_1, _or), {"and": [field_1, _or]})
        self.assertEquals(merge_queries_filters(field_1, _and), {"and": [field_1] + _and["and"]})
