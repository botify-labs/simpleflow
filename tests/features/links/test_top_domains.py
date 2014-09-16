import unittest

import os.path
from cdf.features.links.streams import OutlinksRawStreamDef

from cdf.features.links.top_domains import (
    _group_links,
    group_links_by_domain,
    group_links_by_second_level_domain,
    filter_external_outlinks,
    count_unique_links,
    _compute_top_domains,
    compute_top_domains,
    compute_top_second_level_domains
)


class TopDomainTestCase(unittest.TestCase):
    def setUp(self):
        self.externals = [
            [0, "a", 0, -1, "http://foo.com/bar.html"],
            [0, "a", 0, -1, "http://bar.com/image.jpg"],
            [3, "a", 0, -1, "http://foo.com/qux.css"],
            [4, "a", 0, -1, "http://bar.foo.com/baz.html"],
        ]


class TestExternalLinksFiltering(TopDomainTestCase):
    def test_harness(self):
        to_be_filtered = [
            [0, "canonical", 0, -1, "abc.com"],
            [0, "redirection", 0, -1, "abc.com/abc"],
            [0, "a", 5, -1, "domain.com"]  # internal
        ]
        link_stream = OutlinksRawStreamDef.load_iterator(
            to_be_filtered + self.externals)

        externals = filter_external_outlinks(link_stream)
        result = list(externals)
        self.assertEqual(result, self.externals)


class TestGroupLinks(TopDomainTestCase):
    def test_nominal_case(self):
        #extract file extension
        key = lambda x: os.path.splitext(x[4])[1]
        actual_result = _group_links(iter(self.externals), key=key)

        expected_result = [
            (".css", [[3, "a", 0, -1, "http://foo.com/qux.css"]]),
            (".html", [
                [0, "a", 0, -1, "http://foo.com/bar.html"],
                [4, "a", 0, -1, "http://bar.foo.com/baz.html"]
            ]),
            (".jpg", [[0, "a", 0, -1, "http://bar.com/image.jpg"]])
        ]

        self.assertEqual(expected_result, list(actual_result))


class TestGroupLinksByDomain(TopDomainTestCase):
    def test_nominal_case(self):
        actual_result = group_links_by_domain(iter(self.externals))

        expected_result = [
            ("bar.com", [[0, "a", 0, -1, "http://bar.com/image.jpg"]]),
            ("bar.foo.com", [[4, "a", 0, -1, "http://bar.foo.com/baz.html"]]),
            ("foo.com", [
                [0, "a", 0, -1, "http://foo.com/bar.html"],
                [3, "a", 0, -1, "http://foo.com/qux.css"]
            ]),
        ]

        self.assertEqual(expected_result, list(actual_result))


class TestGroupLinksBySecondLevelDomain(TopDomainTestCase):
    def test_nominal_case(self):
        actual_result = group_links_by_second_level_domain(
            iter(self.externals))

        expected_result = [
            ("bar.com", [[0, "a", 0, -1, "http://bar.com/image.jpg"]]),
            ("foo.com", [
                [0, "a", 0, -1, "http://foo.com/bar.html"],
                [3, "a", 0, -1, "http://foo.com/qux.css"],
                [4, "a", 0, -1, "http://bar.foo.com/baz.html"]
            ]),
        ]

        self.assertEqual(expected_result, list(actual_result))


class TestCountUniqueLinks(unittest.TestCase):
    def test_nominal_case(self):
        external_outlinks = iter([
            [0, "a", 0, -1, "http://foo.com/bar.html"],
            [0, "a", 0, -1, "http://foo.com/baz.html"],
            [3, "a", 0, -1, "http://foo.com/qux.css"],
            [0, "a", 0, -1, "http://foo.com/baz.html"],  # duplicate link
            [3, "a", 0, -1, "http://foo.com/baz.css"]
        ])
        self.assertEqual(4, count_unique_links(external_outlinks))


class Test_ComputeTopNDomains(unittest.TestCase):
    def setUp(self):
        #extract destination url
        self.key = lambda x: x[4]

    def test_nominal_case(self):
        externals = iter([
            [0, "a", 0, -1, "foo.com"],
            [0, "a", 0, -1, "bar.com"],
            [3, "a", 0, -1, "foo.com"],
            [4, "a", 0, -1, "bar.foo.com"],
            [4, "a", 0, -1, "bar.com"],
            [4, "a", 0, -1, "foo.com"],
        ])
        n = 2
        actual_result = _compute_top_domains(externals, n, self.key)
        expected_result = [(3, "foo.com"), (2, "bar.com")]
        self.assertEqual(expected_result, actual_result)

    def test_n_too_big(self):
        externals = iter([
            [0, "a", 0, -1, "foo.com"],
            [0, "a", 0, -1, "bar.com"],
            [4, "a", 0, -1, "foo.com"],
        ])
        n = 10
        actual_result = _compute_top_domains(externals, n, self.key)
        expected_result = [(2, "foo.com"), (1, "bar.com")]
        self.assertEqual(expected_result, actual_result)

    def test_no_follow_links(self):
        externals = iter([
            [0, "a", 1, -1, "bar.com"],  # no follow
            [3, "a", 0, -1, "bar.foo.com"],
            [4, "a", 0, -1, "bar.foo.com"],
            [4, "a", 0, -1, "bar.com"],
        ])
        n = 1
        actual_result = _compute_top_domains(externals, n, self.key)
        expected_result = [(2, "bar.foo.com")]
        self.assertEqual(expected_result, actual_result)

    def test_all_nofollow_links(self):
        #all no follow links
        externals = iter([
            [0, "a", 1, -1, "foo.com"],
            [3, "a", 1, -1, "foo.com"]
        ])
        n = 2
        actual_result = _compute_top_domains(externals, n, self.key)
        expected_result = []
        self.assertEqual(expected_result, actual_result)

    def test_duplicated_links(self):
        externals = iter([
            [0, "a", 0, -1, "foo.com"],
            [0, "a", 0, -1, "bar.com"],
            [3, "a", 0, -1, "foo.com"],
            [4, "a", 0, -1, "bar.foo.com"],
            [4, "a", 0, -1, "bar.com"],
            [4, "a", 0, -1, "foo.com"],
            [0, "a", 0, -1, "foo.com"]  # duplicated link
        ])
        n = 2
        actual_result = _compute_top_domains(externals, n, self.key)
        expected_result = [(3, "foo.com"), (2, "bar.com")]
        self.assertEqual(expected_result, actual_result)


class TestComputeTopNDomains(unittest.TestCase):
    def test_nominal_case(self):
        externals = iter([
            [0, "a", 0, -1, "http://foo.com/bar.html"],
            [0, "a", 0, -1, "http://bar.com/image.jpg"],
            [3, "a", 0, -1, "http://foo.com/qux.css"],
            [4, "a", 0, -1, "http://bar.foo.com/baz.html"],
            [4, "a", 0, -1, "http://bar.com/baz.html"],
            [4, "a", 0, -1, "http://foo.com/"],
        ])
        n = 2
        actual_result = compute_top_domains(externals, n)
        expected_result = [(3, "foo.com"), (2, "bar.com")]
        self.assertEqual(expected_result, actual_result)


class TestComputeTopNSecondLevelDomain(unittest.TestCase):
    def test_nominal_case(self):
        externals = iter([
            [0, "a", 0, -1, "http://foo.com/bar.html"],
            [0, "a", 0, -1, "http://bar.com/image.jpg"],
            [3, "a", 0, -1, "http://foo.com/qux.css"],
            [4, "a", 0, -1, "http://bar.foo.com/baz.html"],
            [4, "a", 0, -1, "http://bar.com/baz.html"],
            [4, "a", 0, -1, "http://foo.com/"],
        ])
        n = 2
        actual_result = compute_top_second_level_domains(externals, n)
        expected_result = [(4, "foo.com"), (2, "bar.com")]
        self.assertEqual(expected_result, actual_result)
