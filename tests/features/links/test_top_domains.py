import unittest

import os.path

from cdf.features.links.top_domains import (
    _group_links,
    group_links_by_domain,
    group_links_by_top_level_domain
)


class TestGroupLinks(unittest.TestCase):
    def test_nominal_case(self):
        link_stream = iter([
            (0, "a", 0, -1, "http://foo.com/bar.html"),
            (0, "a", 0, -1, "http://bar.com/image.jpg"),
            (3, "a", 0, -1, "http://foo.com/qux.css"),
            (4, "a", 0, -1, "http://bar.foo.com/baz.html"),
        ])

        #extract file extension
        key = lambda x: os.path.splitext(x[4])[1]
        actual_result = _group_links(link_stream, key=key)

        expected_result = [
            (".css", [(3, "a", 0, -1, "http://foo.com/qux.css")]),
            (".html", [
                (0, "a", 0, -1, "http://foo.com/bar.html"),
                (4, "a", 0, -1, "http://bar.foo.com/baz.html")
            ]),
            (".jpg", [(0, "a", 0, -1, "http://bar.com/image.jpg")])
        ]

        self.assertEqual(expected_result, list(actual_result))


class TestGroupLinksByDomain(unittest.TestCase):
    def test_nominal_case(self):
        link_stream = iter([
            (0, "a", 0, -1, "http://foo.com/bar.html"),
            (0, "a", 0, -1, "http://bar.com/image.jpg"),
            (3, "a", 0, -1, "http://foo.com/qux.css"),
            (4, "a", 0, -1, "http://bar.foo.com/baz.html"),
        ])

        actual_result = group_links_by_domain(link_stream)

        expected_result = [
            ("bar.com", [(0, "a", 0, -1, "http://bar.com/image.jpg")]),
            ("bar.foo.com", [(4, "a", 0, -1, "http://bar.foo.com/baz.html")]),
            ("foo.com", [
                (0, "a", 0, -1, "http://foo.com/bar.html"),
                (3, "a", 0, -1, "http://foo.com/qux.css")
            ]),
        ]

        self.assertEqual(expected_result, list(actual_result))


class TestGroupLinksByTopLevelDomain(unittest.TestCase):
    def test_nominal_case(self):
        link_stream = iter([
            (0, "a", 0, -1, "http://foo.com/bar.html"),
            (0, "a", 0, -1, "http://bar.com/image.jpg"),
            (3, "a", 0, -1, "http://foo.com/qux.css"),
            (4, "a", 0, -1, "http://bar.foo.com/baz.html"),
        ])

        actual_result = group_links_by_top_level_domain(link_stream)

        expected_result = [
            ("bar.com", [(0, "a", 0, -1, "http://bar.com/image.jpg")]),
            ("foo.com", [
                (0, "a", 0, -1, "http://foo.com/bar.html"),
                (3, "a", 0, -1, "http://foo.com/qux.css"),
                (4, "a", 0, -1, "http://bar.foo.com/baz.html")
            ]),
        ]

        self.assertEqual(expected_result, list(actual_result))
