# -*- coding:utf-8 -*-
import unittest
import logging
from datetime import datetime


from cdf.log import logger
from cdf.collections.url_data.request import UrlRequest

logger.setLevel(logging.DEBUG)


class TestUrlDataGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_filters(self):
        filters = {
            "and": {
                "or": [
                    {"field": "resource_type", "predicate": "starts", "value": "recette/"},
                    #{"field": "resource_type", "predicate": "ends", "value": "permalink"},
                ]
            }
        }
        r = UrlRequest("http://localhost:9200", "marmiton", 2, 2)
        import json
        print json.dumps(r._make_raw_tagging_filters(filters))

        query = {
            "fields": ["url", "resource_type", "metadata.h1"],
            "tagging_filters": {
                "and": [
                    {"not": True, "field": "resource_type", "predicate": "starts", "value": "recette/"}
                ]
            },
            "filters": {
                "and": [
                    {"field": "metadata.h1", "value": "tomate"}
                ]
            }
        }
        print json.dumps(r.make_raw_query(query))
        print r.query(query, start=0, limit=3)

