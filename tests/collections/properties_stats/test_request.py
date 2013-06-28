# -*- coding:utf-8 -*-
import unittest
import logging
from pandas import DataFrame

from cdf.log import logger
from cdf.collections.properties_stats.request import PropertiesStatsRequest

logger.setLevel(logging.DEBUG)


class TestPropertiesStats(unittest.TestCase):

    def setUp(self):
        self.data = [
            {'host': 'www.site.com',
             'content_type': 'text/html',
             'resource_type': 'article',
             'depth': 1,
             'follow': True,
             'index': True,
             'pages_nb': 10,
             'pages_code_200': 4,
             'pages_code_301': 6
             },
            {'host': 'subdomain.site.com',
             'content_type': 'text/html',
             'resource_type': 'photo',
             'depth': 5,
             'follow': True,
             'index': True,
             'pages_nb': 20,
             'pages_code_500': 20
             }
        ]

    def tearDown(self):
        pass

    def test_simple(self):
        df = DataFrame(self.data)
        request = PropertiesStatsRequest(df)
        self.assertEquals(request.fields_sum(['pages_nb', 'pages_code_200', 'pages_code_500']), {'pages_code_500': 20, 'pages_code_200': 4, 'pages_nb': 30})

        filter_1 = {
            'host': 'www.site.com'
        }
        self.assertEquals(request.fields_sum(['pages_nb'], filter_1), {'pages_nb': 10})

        filter_2 = {
            'host': lambda i: i.str.endswith('.site.com')
        }
        self.assertEquals(request.fields_sum(['pages_nb'], filter_2), {'pages_nb': 30})

        filter_3 = {
            'depth': lambda i: i.isin([1, 4])
        }
        self.assertEquals(request.fields_sum(['pages_nb'], filter_3), {'pages_nb': 10})

    def test_sum_by_property_1dim(self):
        df = DataFrame(self.data)
        request = PropertiesStatsRequest(df)

        expected_results = [
            {
                "properties": {
                    "host": "www.site.com"
                },
                "counters": {
                    "pages_nb": 10
                }
            },
            {
                "properties": {
                    "host": "subdomain.site.com"
                },
                "counters": {
                    "pages_nb": 20
                }
            }
        ]
        self.assertItemsEqual(request.fields_sum_by_property(['pages_nb'], merge=['host']), expected_results)

    def test_sum_by_property_2dim(self):
        df = DataFrame(self.data)
        request = PropertiesStatsRequest(df)

        expected_results = [
            {
                "properties": {
                    "host": "www.site.com",
                    "content_type": "text/html"
                },
                "counters": {
                    "pages_nb": 10,
                }
            },
            {
                "properties": {
                    "host": "subdomain.site.com",
                    "content_type": "text/html"
                },
                "counters": {
                    "pages_nb": 20
                }
            }
        ]
        self.assertItemsEqual(request.fields_sum_by_property(['pages_nb'], merge=['host', 'content_type']), expected_results)
