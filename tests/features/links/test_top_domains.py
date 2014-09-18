import unittest

import os.path
from cdf.features.links.streams import OutlinksRawStreamDef

from cdf.features.links.top_domains import (
    _group_links,
    filter_external_outlinks,
    count_unique_links,
    count_unique_follow_links,
    _compute_top_domains,
    compute_top_domains,
    compute_top_second_level_domains,
    DomainLinkStats,
    compute_domain_stats,
    LinkDestination,
    compute_sample_links,
    get_source_sample
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


class TestCountUniqueFollowLinks(unittest.TestCase):
    def test_nominal_case(self):
        external_outlinks = iter([
            [0, "a", 0, -1, "http://foo.com/bar.html"],
            [0, "a", 0, -1, "http://foo.com/baz.html"],
            [3, "a", 0, -1, "http://foo.com/qux.css"],
            [0, "a", 0, -1, "http://foo.com/baz.html"],  # duplicate link
            [3, "a", 0, -1, "http://foo.com/baz.css"],
            [3, "a", 1, -1, "http://foo.com"]  # no follow
        ])
        self.assertEqual(4, count_unique_follow_links(external_outlinks))


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
        expected_result = [
            (3, DomainLinkStats("foo.com", 3, 0, 3)),
            (2, DomainLinkStats("bar.com", 2, 0, 2))
        ]
        self.assertEqual(expected_result, actual_result)

    def test_n_too_big(self):
        externals = iter([
            [0, "a", 0, -1, "foo.com"],
            [0, "a", 0, -1, "bar.com"],
            [4, "a", 0, -1, "foo.com"],
        ])
        n = 10
        actual_result = _compute_top_domains(externals, n, self.key)
        expected_result = [
            (2, DomainLinkStats("foo.com", 2, 0, 2)),
            (1, DomainLinkStats("bar.com", 1, 0, 1))
        ]
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
        expected_result = [(2, DomainLinkStats("bar.foo.com", 2, 0, 2))]
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
        expected_result = [
            (3, DomainLinkStats("foo.com", 4, 0, 3)),
            (2, DomainLinkStats("bar.com", 2, 0, 2))
        ]
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
        expected_result = [
            (3, DomainLinkStats("foo.com", 3, 0, 3)),
            (2, DomainLinkStats("bar.com", 2, 0, 2))
        ]
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
        expected_result = [
            (4, DomainLinkStats("foo.com", 4, 0, 4)),
            (2, DomainLinkStats("bar.com", 2, 0, 2))
        ]
        self.assertEqual(expected_result, actual_result)


class TestDomainStats(unittest.TestCase):
    def setUp(self):
        self.groups = (
            "foo.com",
            [
                [0, "a", 0, -1, "A"],
                [3, "a", 0, -1, "B"],
                [4, "a", 0, -1, "C"],
                [5, "a", 1, -1, "A"],
                [6, "a", 0, -1, "A"],
                [7, "a", 0, -1, "A"],
                # url 8 has 2 follow to A
                # and a nofollow to A
                [8, "a", 2, -1, "A"],
                [8, "a", 0, -1, "A"],
                [8, "a", 0, -1, "A"],
                [9, "a", 0, -1, "A"]
            ]
        )

    def test_link_counts(self):
        result = compute_domain_stats(self.groups).to_dict()
        expected_follow = 8
        expected_nofollow = 2
        self.assertEqual(result['follow_links'], expected_follow)
        self.assertEqual(result['no_follow_links'], expected_nofollow)

    def test_unique_link_counts(self):
        result = compute_domain_stats(self.groups).to_dict()
        expected_unique_follow = 7
        self.assertEqual(result['unique_follow_links'], expected_unique_follow)

    def test_domain_name(self):
        result = compute_domain_stats(self.groups).to_dict()
        expected_domain = 'foo.com'
        self.assertEqual(result['domain'], expected_domain)


class TestComputeSampleLinks(unittest.TestCase):
    def test_nominal_case(self):
        externals = iter([
            [0, "a", 0, -1, "http://foo.com/bar.html"],
            [3, "a", 0, -1, "http://foo.com/qux.css"],
            [3, "a", 0, -1, "http://foo.com/bar.html"],
            [4, "a", 0, -1, "http://foo.com/baz.html"],
            [5, "a", 0, -1, "http://foo.com/baz.html"],
            [4, "a", 0, -1, "http://foo.com/bar.html"],
        ])
        n = 2
        actual_result = compute_sample_links(externals, n)

        expected_result = [(3, LinkDestination("http://foo.com/bar.html", 3, [0, 3, 4])),
                           (2, LinkDestination("http://foo.com/baz.html", 2, [4, 5]))]

        self.assertEqual(expected_result, actual_result)

    def test_unique_links(self):
        externals = iter([
            [0, "a", 0, -1, "http://foo.com/bar.html"],
            [0, "a", 0, -1, "http://foo.com/bar.html"],
            [0, "a", 0, -1, "http://foo.com/bar.html"],
            [0, "a", 0, -1, "http://foo.com/bar.html"],
            [0, "a", 0, -1, "http://foo.com/bar.html"],  # many duplicates
            [3, "a", 0, -1, "http://foo.com/qux.html"],
            [4, "a", 0, -1, "http://foo.com/baz.html"],
            [4, "a", 0, -1, "http://foo.com/qux.html"],
            [5, "a", 0, -1, "http://foo.com/baz.html"],
            [6, "a", 0, -1, "http://foo.com/baz.html"]
        ])
        n = 2
        actual_result = compute_sample_links(externals, n)

        expected_result = [(3, LinkDestination("http://foo.com/baz.html", 3, [4, 5, 6])),
                           (2, LinkDestination("http://foo.com/qux.html", 2, [3, 4]))]
        self.assertEqual(expected_result, actual_result)

    def test_nofollow(self):
        externals = iter([
            [0, "a", 1, -1, "http://foo.com/bar.html"],
            [3, "a", 0, -1, "http://foo.com/qux.css"],
            [3, "a", 0, -1, "http://foo.com/bar.html"],
            [4, "a", 3, -1, "http://foo.com/baz.html"],
            [5, "a", 0, -1, "http://foo.com/baz.html"],
            [4, "a", 5, -1, "http://foo.com/bar.html"],
        ])
        n = 2
        actual_result = compute_sample_links(externals, n)

        expected_result = [(3, LinkDestination("http://foo.com/bar.html", 3, [0, 3, 4])),
                           (2, LinkDestination("http://foo.com/baz.html", 2, [4, 5]))]

        self.assertEqual(expected_result, actual_result)


class TestGetSourceSample(unittest.TestCase):
    def test_nominal_case(self):
        externals = iter([
            [3, "a", 0, -1, "http://foo.com/"],
            [3, "a", 0, -1, "http://foo.com/"],
            [4, "a", 3, -1, "http://foo.com/"],
            [0, "a", 1, -1, "http://foo.com/"],
            [5, "a", 0, -1, "http://foo.com/"],
            [4, "a", 5, -1, "http://foo.com/"]
        ])
        n = 2
        self.assertEqual([0, 3], get_source_sample(externals, n))
