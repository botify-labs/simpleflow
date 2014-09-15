import unittest

import os.path

from cdf.features.links.top_domains import (
    get_domain,
    get_top_level_domain,
    _group_links,
    group_links_by_domain,
    group_links_by_top_level_domain
)


class TestGetDomain(unittest.TestCase):
    def test_nominal_case(self):
        self.assertEqual("foo.com", get_domain("http://foo.com/bar.html"))

    def test_subdomain_case(self):
        self.assertEqual("foo.bar.com", get_domain("http://foo.bar.com/baz.html"))


class TestGetTopLevelDomain(unittest.TestCase):
    def test_nominal_case(self):
        self.assertEqual(
            "foo.com",
            get_top_level_domain("http://foo.com/bar.html")
        )

    def test_subdomain_case(self):
        self.assertEqual(
            "baz.com",
            get_top_level_domain("http://foo.bar.baz.com/baz.html")
        )

    def test_composite_extension(self):
        self.assertEqual(
            "bar.co.uk",
            get_top_level_domain("http://foo.bar.co.uk/baz.html")
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

