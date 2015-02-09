import unittest
import mock
import os.path
import itertools
from cdf.core.streams.cache import BufferedStreamCache

from cdf.features.links.streams import OutlinksRawStreamDef

from cdf.features.links.top_domains import (
    _group_links,
    filter_external_outlinks,
    filter_invalid_destination_urls,
    count_unique_links,
    count_unique_follow_links,
    _compute_top_domains,
    compute_top_full_domains,
    compute_top_second_level_domains,
    DomainLinkStats,
    compute_domain_link_counts,
    LinkDestination,
    compute_sample_links,
    compute_link_destination_stats,
    resolve_sample_url_id,
    TopSecondLevelDomainAggregator,
    _pre_aggregate_link_stream
)


def _get_stream_cache(stream):
    cache = BufferedStreamCache()
    cache.cache(stream)
    return cache


class TestLinkDestination(unittest.TestCase):
    def test_equals(self):
        l1 = LinkDestination('', 2, [0, 1])
        l2 = LinkDestination('', 2, [1, 0])
        self.assertEqual(l1, l2)

    def test_to_dict(self):
        l = LinkDestination('foo.com/a', 2, ['a', 'b'])
        result = l.to_dict()
        expected = {
            "url": 'foo.com/a',
            "unique_links": 2,
            "sources": ['a', 'b']
        }
        self.assertEqual(result, expected)


class TestDomainLinkStats(unittest.TestCase):
    def test_to_dict(self):
        domain_link_stats = DomainLinkStats(
            name="foo.com",
            follow=5,
            nofollow=3,
            follow_unique=2,
            nofollow_unique=1,
            sample_follow_links=[
                LinkDestination("foo.com/1", 1, []),
                LinkDestination("foo.com/2", 2, [])
            ],
            sample_nofollow_links=[
                LinkDestination("foo.com/1", 4, []),
                LinkDestination("foo.com/2", 2, [])
            ]
        )

        expected_result = {
            "domain": "foo.com",
            "unique_follow_links": 2,
            "follow_links": 5,
            "nofollow_links": 3,
            "unique_nofollow_links": 1,
            "follow_samples": [
                #samples are sorted by decreasing unique_links
                {
                    "url": "foo.com/2",
                    "sources": [],
                    "unique_links": 2
                },
                {
                    "url": "foo.com/1",
                    "sources": [],
                    "unique_links": 1
                }
            ],
            "nofollow_samples": [
                {
                    "url": "foo.com/1",
                    "sources": [],
                    "unique_links": 4
                },
                {
                    "url": "foo.com/2",
                    "sources": [],
                    "unique_links": 2
                }
            ]
        }
        self.assertEqual(expected_result, domain_link_stats.to_dict())


class TestFilterInvalidDestinationUrls(unittest.TestCase):
    def test_nominal_case(self):
        externals = iter([
            [0, "a", 0, -1, "http://foo.com/bar.html"],
            [0, "a", 0, -1, "http://bar/image.jpg"],
            [3, "a", 0, -1, "http://foo.com/qux.css"],
            [4, "a", 0, -1, "foo"],
        ])

        actual_result = filter_invalid_destination_urls(externals)
        expected_result = [
            [0, "a", 0, -1, "http://foo.com/bar.html"],
            [3, "a", 0, -1, "http://foo.com/qux.css"]
        ]
        actual_result = list(actual_result)
        self.assertEqual(expected_result, list(actual_result))


class TestPreAggregation(unittest.TestCase):
    def test_pre_aggregation(self):
        externals = [
            [0, 9, "http://foo.com/bar.html"],
            [0, 8, "http://foo.com/bar.html"],
            [0, 8, "http://foo.com/bar.html"],
            [0, 12, "http://foo.com/bar.html"],
            [0, 8, "http://foo.com/bar.html"],
            [3, 8, "http://foo.com/qux.css"],
        ]
        result = list(_pre_aggregate_link_stream(externals))
        expected = [
            (0, True, 'http://foo.com/bar.html', 3),
            (0, False, 'http://foo.com/bar.html', 2),
            (3, True, 'http://foo.com/qux.css', 1)
        ]
        self.assertItemsEqual(result, expected)


class TopDomainTestCase(unittest.TestCase):
    def setUp(self):
        self.externals = [
            [0, "a", 8, -1, "http://foo.com/bar.html"],
            [0, "a", 8, -1, "http://bar.com/image.jpg"],
            [3, "a", 8, -1, "http://foo.com/qux.css"],
            [4, "a", 8, -1, "http://bar.foo.com/baz.html"],
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
            (".css", [[3, "a", 8, -1, "http://foo.com/qux.css"]]),
            (".html", [
                [0, "a", 8, -1, "http://foo.com/bar.html"],
                [4, "a", 8, -1, "http://bar.foo.com/baz.html"]
            ]),
            (".jpg", [[0, "a", 8, -1, "http://bar.com/image.jpg"]])
        ]

        self.assertEqual(
            expected_result,
            [(domain, list(links)) for domain, links in actual_result]
        )


class TestCountUniqueLinks(unittest.TestCase):
    def test_nominal_case(self):
        external_outlinks = iter([
            [0, 0, "http://foo.com/bar.html"],
            [0, 0, "http://foo.com/baz.html"],
            [3, 0, "http://foo.com/qux.css"],
            [0, 0, "http://foo.com/baz.html"], # duplicate link
            [3, 0, "http://foo.com/baz.css"]
        ])
        self.assertEqual(4, count_unique_links(external_outlinks))


class TestCountUniqueFollowLinks(unittest.TestCase):
    def test_nominal_case(self):
        external_outlinks = iter([
            [0, 0, "http://foo.com/bar.html"],
            [0, 0, "http://foo.com/baz.html"],
            [3, 0, "http://foo.com/qux.css"],
            [0, 0, "http://foo.com/baz.html"], # duplicate link
            [3, 0, "http://foo.com/baz.css"],
            [3, 1, "http://foo.com"]  # no follow
        ])
        self.assertEqual(4, count_unique_follow_links(external_outlinks))


class TestTopSecondLevelDomainAggregator(unittest.TestCase):
    def test_nominal_case(self):
        pre_aggregated = iter([
            ['foo.com', 'foo.com/a', 0, True, 2],
            ['foo.com', 'foo.com/a', 1, True, 1],
            ['foo.com', 'foo.com/a', 1, False, 3],
            ['foo.com', 'foo.com/b', 1, True, 5],
        ])
        pre_aggregated = _get_stream_cache(pre_aggregated)
        agg = TopSecondLevelDomainAggregator(n=2)

        agg.merge('foo.com', pre_aggregated)
        result = agg.get_result()
        expected = [
            DomainLinkStats(
                'foo.com', 8, 3, 3, 1,
                [LinkDestination('foo.com/a', 2, [0, 1]),
                 LinkDestination('foo.com/b', 1, [1])],
                [LinkDestination('foo.com/a', 1, [1])]
            )
        ]
        self.assertEqual(expected, result)

    def test_multiple_domains(self):
        pre_aggregated = iter([
            ['bar.com', 'bar.com/a', 0, True, 2],
            ['foo.com', 'foo.com/a', 0, True, 2],
            ['foo.com', 'foo.com/a', 1, True, 2],
        ])
        agg = TopSecondLevelDomainAggregator(n=2)
        # patch the sample part
        agg._compute_sample_links = lambda self, x: []

        for domain, group in itertools.groupby(pre_aggregated, lambda x: x[0]):
            group = _get_stream_cache(group)
            agg.merge(domain, group)

        result = agg.get_result()
        expected = [
            DomainLinkStats(
                'foo.com', 4, 0, 2, 0, []
            ),
            DomainLinkStats(
                'bar.com', 2, 0, 1, 0, []
            ),
        ]
        self.assertEqual(expected, result)

    def test_top_domains(self):
        pre_aggregated = iter([
            ['bar.com', 'bar.com/a', 0, True, 150],
            ['bar.com', 'bar.com/a', 1, False, 150],
            ['bar.com', 'bar.com/a', 2, False, 150],
            ['bar.com', 'bar.com/a', 3, False, 150],
            ['foobar.com', 'foobar.com/a', 0, True, 9],
            ['foobar.com', 'foobar.com/b', 0, True, 2],
            ['foo.com', 'foo.com/a', 0, True, 1],
            ['foo.com', 'foo.com/b', 0, True, 1],
            ['foo.com', 'foo.com/c', 0, True, 1],
        ])
        agg = TopSecondLevelDomainAggregator(n=1)
        # patch the sample part
        agg._compute_sample_links = lambda self, x: []

        for domain, group in itertools.groupby(pre_aggregated, lambda x: x[0]):
            group = _get_stream_cache(group)
            agg.merge(domain, group)

        result = agg.get_result()
        expected = [
            DomainLinkStats(
                'foo.com', 3, 0, 3, 0, []
            ),
        ]
        self.assertEqual(expected, result)


class TestComputeTopNDomains(unittest.TestCase):
    def test_nominal_case(self):
        externals = iter([
            [0, 0, "foo.com"],
            [0, 0, "bar.com"],
            [3, 0, "foo.com"],
            [4, 0, "bar.foo.com"],
            [4, 0, "bar.com"],
            [4, 0, "foo.com"],
        ])
        n = 2
        actual_result = compute_top_full_domains(externals, n)
        expected_result = [
            DomainLinkStats(
                "foo.com", 3, 0, 3, 0,
                [LinkDestination("foo.com", 3, [0, 3, 4])],
                []
            ),
            DomainLinkStats(
                "bar.com", 2, 0, 2, 0,
                [LinkDestination("bar.com", 2, [0, 4])],
                []
            )
        ]
        self.assertEqual(expected_result, actual_result)

    def test_nominal_case1(self):
        externals = iter([
            [0, 0, "http://foo.com/bar.html"],
            [0, 0, "http://bar.com/image.jpg"],
            [3, 0, "http://foo.com/qux.css"],
            [4, 0, "http://bar.foo.com/baz.html"],
            [4, 0, "http://bar.com/baz.html"],
            [4, 0, "http://foo.com/"],
        ])
        n = 2
        actual_result = compute_top_full_domains(externals, n)
        expected_result = [
            DomainLinkStats(
                "foo.com", 3, 0, 3, 0,
                [
                    LinkDestination("http://foo.com/", 1, [4]),
                    LinkDestination("http://foo.com/bar.html", 1, [0]),
                    LinkDestination("http://foo.com/qux.css", 1, [3]),
                ]
            ),
            DomainLinkStats(
                "bar.com", 2, 0, 2, 0,
                [
                    LinkDestination("http://bar.com/baz.html", 1, [4]),
                    LinkDestination("http://bar.com/image.jpg", 1, [0])
                ]
            )
        ]
        self.assertEqual(expected_result, actual_result)

    def test_nofollow_links(self):
        externals = iter([
            [0, 1, "foo.com"],
            [3, 0, "foo.com"],
            [4, 3, "foo.com"],
        ])
        n = 1
        actual_result = _compute_top_domains(externals, n, self.key)
        expected_result = [
            DomainLinkStats(
                "foo.com", 1, 2, 1,
                [LinkDestination("foo.com", 1, [3])],
                [LinkDestination("foo.com", 2, [0, 4])]
            )
        ]
        self.assertEqual(expected_result, actual_result)

    @mock.patch("cdf.features.links.top_domains.compute_sample_links", autospec=True)
    def test_n_too_big(self, compute_sample_links_mock):
        #mock to make expected_result easier to understand
        compute_sample_links_mock.return_value = []

        externals = iter([
            [0, 0, "foo.com"],
            [0, 0, "bar.com"],
            [4, 0, "foo.com"],
        ])
        n = 10
        actual_result = _compute_top_domains(externals, n, self.key)
        expected_result = [
            DomainLinkStats("foo.com", 2, 0, 2, 0, []),
            DomainLinkStats("bar.com", 1, 0, 1, 0, [])
        ]
        self.assertEqual(expected_result, actual_result)

    @mock.patch("cdf.features.links.top_domains.compute_sample_links", autospec=True)
    def test_nofollow_links(self, compute_sample_links_mock):
        #mock to make expected_result easier to understand
        compute_sample_links_mock.return_value = []

        externals = iter([
            [0, 1, "bar.com"], # no follow
            [3, 0, "bar.foo.com"],
            [4, 0, "bar.foo.com"],
            [4, 0, "bar.com"],
        ])
        n = 1
        actual_result = _compute_top_domains(externals, n, self.key)
        expected_result = [
            DomainLinkStats("bar.foo.com", 2, 0, 2, 0, [])
        ]
        self.assertEqual(expected_result, actual_result)

    def test_all_nofollow_links(self):
        #all no follow links
        externals = iter([
            [0, 1, "foo.com"]
        ])
        n = 2
        actual_result = compute_top_full_domains(externals, n)
        expected_result = [
            DomainLinkStats("foo.com", 0, 1, 0, 1, [], [
                LinkDestination("foo.com", 1, [0])
            ])
        ]
        self.assertEqual(expected_result, actual_result)

    @mock.patch("cdf.features.links.top_domains.compute_sample_links", autospec=True)
    def test_duplicated_links(self, compute_sample_links_mock):
        #mock to make expected_result easier to understand
        compute_sample_links_mock.return_value = []

        externals = iter([
            [0, 0, "foo.com"],
            [0, 0, "bar.com"],
            [3, 0, "foo.com"],
            [4, 0, "bar.foo.com"],
            [4, 0, "bar.com"],
            [4, 0, "foo.com"],
            [0, 0, "foo.com"]  # duplicated link
        ])
        n = 2
        actual_result = compute_top_full_domains(externals, n)
        expected_result = [
            DomainLinkStats("foo.com", 4, 0, 3, 0, []),
            DomainLinkStats("bar.com", 2, 0, 2, 0, [])
        ]
        self.assertEqual(expected_result, actual_result)


class TestComputeTopNSecondLevelDomain(unittest.TestCase):
    def test_nominal_case(self):
        externals = iter([
            [0, 0, "http://foo.com/bar.html"],
            [0, 0, "http://bar.com/image.jpg"],
            [3, 0, "http://foo.com/qux.css"],
            [4, 0, "http://bar.foo.com/baz.html"],
            [4, 0, "http://bar.com/baz.html"],
            [4, 0, "http://foo.com/"],
        ])
        n = 2
        actual_result = compute_top_second_level_domains(externals, n)
        expected_result = [
            DomainLinkStats(
                "foo.com", 4, 0, 4, 0,
                [
                    LinkDestination("http://foo.com/bar.html", 1, [0]),
                    LinkDestination("http://foo.com/qux.css", 1, [3]),
                    LinkDestination("http://foo.com/", 1, [4]),
                    LinkDestination("http://bar.foo.com/baz.html", 1, [4])
                ]
            ),
            DomainLinkStats(
                "bar.com", 2, 0, 2, 0,
                [
                    LinkDestination("http://bar.com/image.jpg", 1, [0]),
                    LinkDestination("http://bar.com/baz.html", 1, [4]),
                ]
            )
        ]
        self.assertEqual(expected_result, actual_result)


class TestDomainLinkCounts(unittest.TestCase):
    def setUp(self):
        self.groups = (
            "foo.com",
            [
                [0, 0, "A"],
                [3, 0, "B"],
                [4, 0, "C"],
                [5, 1, "A"],
                [6, 0, "A"],
                [5, 1, "A"],
                [7, 0, "A"],
                # url 8 has 2 follow to A
                # and a nofollow to A
                [8, 2, "A"],
                [9, 0, "A"],
                [8, 0, "A"],
                [8, 0, "A"],

            ]
        )

    def test_link_counts(self):
        result = compute_domain_link_counts(self.groups).to_dict()
        expected_follow = 8
        expected_nofollow = 3
        self.assertEqual(result['follow_links'], expected_follow)
        self.assertEqual(result['nofollow_links'], expected_nofollow)

    def test_unique_link_counts(self):
        result = compute_domain_link_counts(self.groups).to_dict()
        expected_unique_follow = 7
        self.assertEqual(result['unique_follow_links'], expected_unique_follow)
        expected_unique_nofollow = 2
        self.assertEqual(result['unique_nofollow_links'], expected_unique_nofollow)

    def test_domain_name(self):
        result = compute_domain_link_counts(self.groups).to_dict()
        expected_domain = 'foo.com'
        self.assertEqual(result['domain'], expected_domain)


class TestTopSecondLevelDomainAggregatorHelpers(unittest.TestCase):
    def setUp(self):
        self.agg = TopSecondLevelDomainAggregator(n=2)

    def test_sample_links_nominal(self):
        link_group_stream = iter([
            ['d', 'url1', 1, True, 1],
            ['d', 'url1', 2, True, 1],
            ['d', 'url2', 1, True, 1],
            ['d', 'url2', 2, True, 1],
            ['d', 'url2', 3, True, 1],
            ['d', 'url2', 4, True, 1],
        ])
        results = self.agg._compute_sample_links(link_group_stream, 3)
        expected = [
            LinkDestination('url2', 4, [1, 2, 3]),
            LinkDestination('url1', 2, [1, 2]),
        ]
        self.assertEqual(results, expected)

    def test_sample_sets_mixed(self):
        link_group_stream = iter([
            ['d', 'url1', 1, True, 1],
            ['d', 'url1', 1, False, 1],
            ['d', 'url1', 2, False, 1],
            ['d', 'url2', 2, True, 1],
            ['d', 'url2', 3, True, 1],
            ['d', 'url4', 4, False, 1],
        ])
        cache = _get_stream_cache(link_group_stream)

        follow_result, nofollow_result = self.agg._compute_sample_sets(
            cache
        )

        follow_expected = [
            LinkDestination('url2', 2, [2, 3]),
            LinkDestination('url1', 1, [1]),
        ]
        nofollow_expected = [
            LinkDestination('url1', 2, [1, 2]),
            LinkDestination('url4', 1, [4]),
        ]

        self.assertEqual(follow_result, follow_expected)
        self.assertEqual(nofollow_result, nofollow_expected)


class TestComputeSampleLinks(unittest.TestCase):
    def test_nominal_case(self):
        externals = iter([
            [0, 0, "http://foo.com/bar.html"],
            [3, 0, "http://foo.com/qux.css"],
            [3, 0, "http://foo.com/bar.html"],
            [4, 0, "http://foo.com/baz.html"],
            [5, 0, "http://foo.com/baz.html"],
            [4, 0, "http://foo.com/bar.html"],
        ])
        n = 2
        actual_result = compute_sample_links(externals, n)

        expected_result = [LinkDestination("http://foo.com/bar.html", 3, [0, 3, 4]),
                           LinkDestination("http://foo.com/baz.html", 2, [4, 5])]

        self.assertEqual(expected_result, actual_result)

    def test_unique_links(self):
        externals = iter([
            [0, 0, "http://foo.com/bar.html"],
            [0, 0, "http://foo.com/bar.html"],
            [0, 0, "http://foo.com/bar.html"],
            [0, 0, "http://foo.com/bar.html"],
            [0, 0, "http://foo.com/bar.html"], # many duplicates
            [3, 0, "http://foo.com/qux.html"],
            [4, 0, "http://foo.com/baz.html"],
            [4, 0, "http://foo.com/qux.html"],
            [5, 0, "http://foo.com/baz.html"],
            [6, 0, "http://foo.com/baz.html"]
        ])
        n = 2
        actual_result = compute_sample_links(externals, n)

        expected_result = [LinkDestination("http://foo.com/baz.html", 3, [4, 5, 6]),
                           LinkDestination("http://foo.com/qux.html", 2, [3, 4])]
        self.assertEqual(expected_result, actual_result)

    def test_nofollow(self):
        externals = iter([
            [0, 1, "http://foo.com/bar.html"],
            [3, 0, "http://foo.com/qux.css"],
            [3, 0, "http://foo.com/bar.html"],
            [4, 3, "http://foo.com/baz.html"],
            [5, 0, "http://foo.com/baz.html"],
            [4, 5, "http://foo.com/bar.html"],
        ])
        n = 2
        actual_result = compute_sample_links(externals, n)

        expected_result = [LinkDestination("http://foo.com/bar.html", 3, [0, 3, 4]),
                           LinkDestination("http://foo.com/baz.html", 2, [4, 5])]

        self.assertEqual(expected_result, actual_result)


class TestComputeLinkDestinationStats(unittest.TestCase):
    def test_nominal_case(self):
        externals = iter([
            [3, 0, "http://foo.com/"],
            [3, 0, "http://foo.com/"],
            [4, 3, "http://foo.com/"],
            [0, 1, "http://foo.com/"],
            [5, 0, "http://foo.com/"],
            [4, 5, "http://foo.com/"]
        ])
        n = 2
        actual_result = compute_link_destination_stats(
            externals,
            "http://foo.com/",
            n
        )
        expected_result = LinkDestination("http://foo.com/", 4, [0, 3])
        self.assertEqual(expected_result, actual_result)


class TestSourceSampleUrl(unittest.TestCase):
    def test_harness(self):
        results = [
            DomainLinkStats(
                '', 0, 0, 0, 0,
                [LinkDestination('', 0, [1, 2, 5])]
            ),
            DomainLinkStats(
                '', 0, 0, 0, 0,
                [LinkDestination('', 0, [4, 3])]
            )
        ]

        ids = iter([
            [0, "http", "host.com", "/url0", ""],
            [1, "http", "host.com", "/url1", ""],
            [2, "http", "host.com", "/url2", ""],
            [3, "http", "host.com", "/url3", ""],
            [4, "http", "host.com", "/url4", ""],
            [5, "http", "host.com", "/url5", ""]
        ])

        resolve_sample_url_id(ids, results)

        expected_0 = LinkDestination(
            '', 0, ['http://host.com/url1', 'http://host.com/url2', 'http://host.com/url5']
        )
        expected_1 = LinkDestination(
            '', 0, ['http://host.com/url4', 'http://host.com/url3']
        )

        self.assertEqual(
            results[0].sample_follow_links,
            [expected_0]
        )
        self.assertEqual(
            results[1].sample_follow_links,
            [expected_1]
        )
