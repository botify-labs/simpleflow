# -*- coding:utf-8 -*-
import unittest
import logging


from cdf.log import logger
from cdf.collections.urls.generators.suggestions import UrlSuggestionsGenerator

logger.setLevel(logging.DEBUG)


class TestUrlSuggestionsGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple(self):
        patterns = (
            [1, 'http', 'www.site.com', '/path/name.html', ''],
            [2, 'http', 'www.site.com', '/another_url.html', '?sid=3JEHG'],
        )

        clusters = {
            'path': [
                'STARTS(path, "/path/")',
                'ENDS(path, ".html")'
            ],
            'qs_key': [
                'query_string = ""',
                '"sid" in query_string_keys',
            ]
        }

        u = UrlSuggestionsGenerator(iter(patterns), clusters)
        results = list(u)
        self.assertEquals(results[0], (1, 'path', 'STARTS(path, "/path/")'))
        self.assertEquals(results[1], (1, 'path', 'ENDS(path, ".html")'))
        self.assertEquals(results[2], (1, 'qs_key', 'query_string = ""'))
        self.assertEquals(results[3], (2, 'path', 'ENDS(path, ".html")'))
        self.assertEquals(results[4], (2, 'qs_key', '"sid" in query_string_keys'))
