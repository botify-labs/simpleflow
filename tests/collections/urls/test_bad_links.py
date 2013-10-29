# -*- coding:utf-8 -*-
import unittest
import logging
from cdf.collections.urls.generators.bad_links import get_bad_links

class TestBadLink(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_bad_links(self):
        stream_infos = iter((
            [1, 0, '', 1, 12345, 200, 1, 1, 1],
            [2, 0, '', 1, 12345, 301, 1, 1, 1],
            [3, 0, '', 1, 12345, 500, 1, 1, 1],
        ))

        stream_outlinks = iter((
            [4, 'a', 0, 1],
            [4, 'a', 0, 2],
            [5, 'a', 0, 1],
            [5, 'a', 0, 3],
            [6, 'canonical', 0, 2],
        ))

        u = get_bad_links(stream_patterns, stream_contents)
        results = list(u)
        expected = [
            (4, 2, 301),
            (5, 3, 500)
        ]
        self.assertEquals(results, expected)
