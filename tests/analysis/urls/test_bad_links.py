# -*- coding:utf-8 -*-
import unittest
from cdf.analysis.urls.generators.bad_links import get_bad_links, get_bad_link_counters

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

        u = get_bad_links(stream_infos, stream_outlinks)
        results = list(u)
        expected = [
            (4, 2, 301),
            (5, 3, 500)
        ]
        self.assertEquals(results, expected)

    def test_bad_link_counters(self):
        stream_bad_links = iter((
            [1, 2, 500],
            [1, 9, 500],
            [1, 2, 400],
            [2, 5, 500],
            [2, 9, 500],
            [3, 6, 300],
            [3, 7, 400],
            [3, 9, 400],
        ))

        u = get_bad_link_counters(stream_bad_links)
        # counters are sorted only on *id*
        results = sorted(list(u), key=lambda record: (record[0], record[1]))
        expected = [
            (1, 400, 1),
            (1, 500, 2),
            (2, 500, 2),
            (3, 300, 1),
            (3, 400, 2)
        ]
        self.assertEquals(results, expected)
