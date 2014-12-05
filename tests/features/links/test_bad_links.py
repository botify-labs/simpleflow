# -*- coding:utf-8 -*-
import unittest
from cdf.features.links.streams import OutlinksStreamDef
from cdf.features.main.reasons import (
    encode_reason_mask,
    REASON_HTTP_CODE
)
from cdf.features.links.bad_links import (
    get_bad_links,
    get_bad_link_counters,
    get_links_to_non_compliant_urls,
    get_link_to_non_compliant_urls_counters
)
from cdf.features.links.helpers.masks import follow_mask


class TestBadLink(unittest.TestCase):
    def setUp(self):
        self.infos = iter((
            [1, 0, '', 1, 12345, 200, 1, 1, 1],
            [2, 0, '', 1, 12345, 301, 1, 1, 1],
            [3, 0, '', 1, 12345, 500, 1, 1, 1],
        ))

    def test_bad_links_harness(self):
        stream_outlinks = OutlinksStreamDef.load_iterator(
            [
                [4, 'a', 0, 1, ""],
                [4, 'a', 0, 2, ""],
                [5, 'a', 0, 1, ""],
                [5, 'a', 0, 3, ""],
                [5, 'a', 4, 3, ""],
                [6, 'canonical', 0, 2, ""],
            ]
        )

        u = get_bad_links(self.infos, stream_outlinks)
        results = list(u)
        expected = [
            (4, 2, 1, 301),
            (5, 3, 1, 500),
            (5, 3, 0, 500)
        ]
        expected_order = [4, 5, 5]
        self.assertEqual([i[0] for i in results], expected_order)
        self.assertItemsEqual(results, expected)

    def test_bad_links_follow(self):
        stream_outlinks = OutlinksStreamDef.load_iterator(
            [
                [4, 'a', 0, 1, ""],
                [4, 'a', 0, 2, ""],
                [4, 'a', 0, 2, ""],  # duplicated
                [4, 'a', 1, 2, ""],  # nofollow
                [4, 'a', 4, 3, ""],
            ]
        )
        u = get_bad_links(self.infos, stream_outlinks)
        results = list(u)
        expected = [
            (4, 2, 1, 301),
            (4, 2, 1, 301),
            (4, 2, 0, 301),
            (4, 3, 0, 500),
        ]
        self.assertItemsEqual(results, expected)

    def test_bad_link_counters_harness(self):
        stream_bad_links = iter((
            [1, 2, 1, 500],
            [1, 9, 1, 500],
            [1, 2, 1, 400],
            [2, 5, 1, 500],
            [2, 9, 1, 500],
            [3, 6, 1, 300],
            [3, 7, 1, 400],
            [3, 9, 1, 400],
        ))

        u = get_bad_link_counters(stream_bad_links)
        # counters are sorted only on *id*
        results = list(u)
        expected = [
            (1, 400, 1),
            (1, 500, 2),
            (2, 500, 2),
            (3, 300, 1),
            (3, 400, 2)
        ]
        self.assertItemsEqual(results, expected)

    def test_bad_link_counters_nofollow_unique(self):
        stream_bad_links = iter((
            [2, 5, 1, 301],  # 301 follow
            [2, 9, 0, 301],  # 301 nofollow
            [2, 9, 1, 500],  # 500 follow
            [2, 9, 1, 500],  # 500 duplicated
            [2, 9, 0, 500],  # 500 nofollow        zzz
        ))

        u = get_bad_link_counters(stream_bad_links)
        # counters are sorted only on *id*
        results = list(u)
        expected = [
            (2, 301, 1),
            (2, 500, 1)
        ]
        self.assertItemsEqual(results, expected)


class TestGetLinkToNonCompliantUrls(unittest.TestCase):
    def setUp(self):
        self.stream_compliant = iter([
            (1, True, encode_reason_mask()),
            (2, True, encode_reason_mask()),
            (3, False, encode_reason_mask(REASON_HTTP_CODE))
        ])

    def test_harness(self):
        stream_outlinks = iter([
            (1, 'a', follow_mask(0), 2),  # compliant
            (1, 'a', follow_mask(0), 3),
            (1, 'a', follow_mask(0), 3),
            (1, 'a', follow_mask(4), 3)   # nofollow link
        ])
        actual_result = get_links_to_non_compliant_urls(
            self.stream_compliant,
            stream_outlinks
        )

        expected_result = [
            (1, 1, 3),
            (1, 1, 3),
            (1, 0, 3),
        ]
        self.assertEqual(expected_result, list(actual_result))


class TestGetLinkToNonCompliantUrlsCounters(unittest.TestCase):
    def test_nominal_case(self):
        stream_non_compliant_links = iter([
            (1, 1, 3),
            (1, 1, 5),
            (1, 0, 5),  # ignored
            (1, 0, 5),  # ignored
            (5, 1, 10),
            (5, 1, 10),
            (5, 1, 10),
            (5, 1, 11),
            (5, 1, 12)
        ])

        actual_result = get_link_to_non_compliant_urls_counters(
            stream_non_compliant_links
        )
        expected_result = [
            (1, 2, 2),
            (5, 3, 5)
        ]
        self.assertEqual(expected_result, list(actual_result))
