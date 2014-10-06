import unittest

from cdf.features.links.percentiles import generate_follow_inlinks_stream


class TestGenerateFollowInlinksStream(unittest.TestCase):
    def setUp(self):
        self.urlids_stream = iter([
            (1, "http", "foo.com", "/"),
            (2, "http", "foo.com", "/index.html"),
            (3, "http", "foo.com", "/bar"),
            (4, "http", "foo.com", "/baz"),
            (5, "http", "foo.com", "/qux"),
            (6, "http", "foo.com", "/barbar"),
        ])
        self.max_crawled_urlid = 6

    def test_nominal_case(self):
        inlinks_count_stream = iter([
            (1, 0, 10,  10),
            (2, 0, 2, 2),
            (3, 0, 1, 1),
            (4, 0, 6, 6),
            (5, 0, 5, 5),
            (6, 0, 8, 8)
        ])
        actual_result = generate_follow_inlinks_stream(self.urlids_stream,
                                                       inlinks_count_stream,
                                                       self.max_crawled_urlid)
        expected_result = [
            (1, 10),
            (2, 2),
            (3, 1),
            (4, 6),
            (5, 5),
            (6, 8)
        ]
        self.assertEqual(expected_result, list(actual_result))

    def test_nofollow_links(self):
        inlinks_count_stream = iter([
            (1, 0, 10,  10),
            (1, 2, 1,  1),
            (2, 0, 2, 2),
            (3, 0, 1, 1),
            (3, 3, 2, 2),
            (4, 0, 6, 6),
            (5, 0, 5, 5),
            (6, 0, 8, 8)
        ])
        actual_result = generate_follow_inlinks_stream(self.urlids_stream,
                                                       inlinks_count_stream,
                                                       self.max_crawled_urlid)
        expected_result = [
            (1, 10),
            (2, 2),
            (3, 1),
            (4, 6),
            (5, 5),
            (6, 8)
        ]
        self.assertEqual(expected_result, list(actual_result))

    def test_missing_urlid(self):
        inlinks_count_stream = iter([
            (1, 0, 10,  10),
            (2, 0, 2, 2),
            (3, 0, 1, 1),
            (5, 0, 5, 5),
            (6, 0, 8, 8)
        ])
        actual_result = generate_follow_inlinks_stream(self.urlids_stream,
                                                       inlinks_count_stream,
                                                       self.max_crawled_urlid)
        expected_result = [
            (1, 10),
            (2, 2),
            (3, 1),
            (4, 0),
            (5, 5),
            (6, 8)
        ]
        self.assertEqual(expected_result, list(actual_result))

    def test_missing_urlids(self):
        inlinks_count_stream = iter([
            (1, 0, 10,  10),
            (3, 0, 1, 1),
            (6, 0, 8, 8)
        ])

        urlids = iter([
            (1, "http", "foo.com", "/"),
            #urlid 2 does not exist
            (3, "http", "foo.com", "/bar"),
            (4, "http", "foo.com", "/baz"),
            (5, "http", "foo.com", "/qux"),
            (6, "http", "foo.com", "/barbar"),
        ])
        actual_result = generate_follow_inlinks_stream(urlids,
                                                       inlinks_count_stream,
                                                       self.max_crawled_urlid)
        expected_result = [
            (1, 10),
            (3, 1),
            (4, 0),
            (5, 0),
            (6, 8)
        ]
        self.assertEqual(expected_result, list(actual_result))

    def test_max_crawled_urlid(self):
        inlinks_count_stream = iter([
            (1, 0, 10,  10),
            (2, 0, 2, 2),
            (3, 0, 1, 1),
            (4, 0, 6, 6),
            (5, 0, 5, 5),
            (6, 0, 8, 8)
        ])
        max_crawled_urlid = 4
        actual_result = generate_follow_inlinks_stream(self.urlids_stream,
                                                       inlinks_count_stream,
                                                       max_crawled_urlid)
        expected_result = [
            (1, 10),
            (2, 2),
            (3, 1),
            (4, 6)
        ]
        self.assertEqual(expected_result, list(actual_result))
