import unittest
import os.path
import itertools
from cdf.core.streams.cache import BufferedStreamCache

from cdf.features.links.streams import OutlinksRawStreamDef

from cdf.features.links.top_domains import (
    _group_links,
    filter_external_outlinks,
    filter_invalid_destination_urls,
    DomainLinkStats,
    LinkDestination,
    resolve_sample_url_id,
    TopSecondLevelDomainAggregator,
    _pre_aggregate_link_stream,
    TopLevelDomainAggregator
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


class TestTopLevelDomainAggregator(unittest.TestCase):
    def test_nominal_case(self):
        pre_aggregated = iter([
            ['foo.com', "http://foo.com/bar.html", 0, True, 1],
            ['foo.com', "http://bar.foo.com/baz.html", 0, True, 1],
            ['foo.com', "http://bar.foo.com/baz.html", 4, True, 1],
            ['foo.com', "http://foo.com/qux.css", 3, True, 1],
            ['foo.com', "http://bar.foo.com/abc.html", 4, True, 1],
        ])
        cache = _get_stream_cache(pre_aggregated)
        agg = TopLevelDomainAggregator(n=2)

        agg.merge('foo.com', cache)
        result = agg.get_result()
        expected = [
            DomainLinkStats(
                'bar.foo.com', 3, 0, 3, 0,
                [
                    LinkDestination('http://bar.foo.com/baz.html', 2, [0, 4]),
                    LinkDestination('http://bar.foo.com/abc.html', 1, [4]),
                ]
            ),
            DomainLinkStats(
                'foo.com', 2, 0, 2, 0,
                [
                    LinkDestination('http://foo.com/qux.css', 1, [3]),
                    LinkDestination('http://foo.com/bar.html', 1, [0]),
                ]
            ),
        ]

        self.assertEqual(expected, result)


class TestTopDomainAggregatorHelpers(unittest.TestCase):
    def setUp(self):
        self.agg = TopSecondLevelDomainAggregator(n=2)

    def test_link_counts_follow(self):
        link_group_stream = iter([
            ['d', 'url1', 1, True, 1],
            ['d', 'url1', 2, True, 1],
            ['d', 'url2', 1, True, 1],
        ])
        result = self.agg._compute_link_counts('d', link_group_stream)
        expected = DomainLinkStats(
            'd', 3, 0, 3, 0
        )
        self.assertEqual(result, expected)

    def test_link_counts_mixed(self):
        link_group_stream = iter([
            ['d', 'url1', 1, True, 1],
            ['d', 'url1', 2, False, 2],
            ['d', 'url2', 1, True, 12],
            ['d', 'url1', 2, False, 5],
            ['d', 'url4', 1, True, 1],
        ])
        result = self.agg._compute_link_counts('d', link_group_stream)
        expected = DomainLinkStats(
            'd', 14, 7, 3, 2
        )
        self.assertEqual(result, expected)

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

    def test_sample_links_ordering(self):
        link_group_stream = iter([
            ['d', "http://foo.com/", 3, True, 2],
            ['d', "http://foo.com/", 4, True, 2],
            ['d', "http://foo.com/", 0, True, 1],
            ['d', "http://foo.com/", 5, True, 1],
        ])
        results = self.agg._compute_sample_links(link_group_stream, 3)
        expected = [LinkDestination("http://foo.com/", 4, [0, 3, 4])]

        self.assertEqual(expected, results)

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
