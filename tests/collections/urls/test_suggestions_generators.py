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

        infos = (
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [2, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        )

        # 1 = title, 2=h1, 3=H2, 4=Meta desc
        contents = [
            [1, 2, 0, 'Teaser : Next episode'],
            [1, 3, 0, '33 comments about this teaser'],
            [2, 2, 0, 'Teaser : First episode'],
        ]

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

        u = UrlSuggestionsGenerator(iter(patterns), iter(infos), iter(contents))
        u.add_pattern_cluster('path', clusters['path'])
        u.add_pattern_cluster('qs_key', clusters['qs_key'])
        u.add_metadata_cluster('h1', [
            'STARTS(h1, "Teaser :")',
        ])
        u.add_metadata_cluster('h2', [
            'CONTAINS(h2, " comments about ")',
        ])
        results = list(u)
        print results
        self.assertEquals(results[0], (1, 'path', 'STARTS(path, "/path/")'))
        self.assertEquals(results[1], (1, 'path', 'ENDS(path, ".html")'))
        self.assertEquals(results[2], (1, 'qs_key', 'query_string = ""'))
        self.assertEquals(results[3], (1, 'metadata_h1', 'STARTS(h1, "Teaser :")'))
        self.assertEquals(results[4], (1, 'metadata_h2', 'CONTAINS(h2, " comments about ")'))
        self.assertEquals(results[5], (2, 'path', 'ENDS(path, ".html")'))
        self.assertEquals(results[6], (2, 'qs_key', '"sid" in query_string_keys'))
        self.assertEquals(results[7], (2, 'metadata_h1', 'STARTS(h1, "Teaser :")'))
