import unittest
import random

from cdf.features.links.percentiles import (
    generate_follow_inlinks_stream,
    PercentileStats,
    compute_percentile_stats,
    compute_quantiles
)


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
        self.inredirections_stream = iter([
        ])
        self.max_crawled_urlid = 6

    def test_nominal_case(self):
        inlinks_count_stream = iter([
            (1, ["follow"], 10,  10),
            (2, ["follow"], 2, 2),
            (3, ["follow"], 1, 1),
            (4, ["follow"], 6, 6),
            (5, ["follow"], 5, 5),
            (6, ["follow"], 8, 8)
        ])

        actual_result = generate_follow_inlinks_stream(
            self.urlids_stream,
            inlinks_count_stream,
            self.inredirections_stream,
            self.max_crawled_urlid
        )
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
            (1, ["follow"], 10,  10),
            (1, ["meta"], 1,  1),
            (2, ["follow"], 2, 2),
            (3, ["follow"], 1, 1),
            (3, ["meta", "link"], 2, 2),
            (4, ["follow"], 6, 6),
            (5, ["follow"], 5, 5),
            (6, ["follow"], 8, 8)
        ])
        actual_result = generate_follow_inlinks_stream(
            self.urlids_stream,
            inlinks_count_stream,
            self.inredirections_stream,
            self.max_crawled_urlid
        )
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
            (1, ["follow"], 10,  10),
            (2, ["follow"], 2, 2),
            (3, ["follow"], 1, 1),
            (5, ["follow"], 5, 5),
            (6, ["follow"], 8, 8)
        ])
        actual_result = generate_follow_inlinks_stream(
            self.urlids_stream,
            inlinks_count_stream,
            self.inredirections_stream,
            self.max_crawled_urlid
        )
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
            (1, ["follow"], 10,  10),
            (3, ["follow"], 1, 1),
            (6, ["follow"], 8, 8)
        ])

        urlids = iter([
            (1, "http", "foo.com", "/"),
            #urlid 2 does not exist
            (3, "http", "foo.com", "/bar"),
            (4, "http", "foo.com", "/baz"),
            (5, "http", "foo.com", "/qux"),
            (6, "http", "foo.com", "/barbar"),
        ])
        actual_result = generate_follow_inlinks_stream(
            urlids,
            inlinks_count_stream,
            self.inredirections_stream,
            self.max_crawled_urlid
        )
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
            (1, ["follow"], 10,  10),
            (2, ["follow"], 2, 2),
            (3, ["follow"], 1, 1),
            (4, ["follow"], 6, 6),
            (5, ["follow"], 5, 5),
            (6, ["follow"], 8, 8)
        ])
        max_crawled_urlid = 4
        actual_result = generate_follow_inlinks_stream(
            self.urlids_stream,
            inlinks_count_stream,
            self.inredirections_stream,
            max_crawled_urlid
        )
        expected_result = [
            (1, 10),
            (2, 2),
            (3, 1),
            (4, 6)
        ]
        self.assertEqual(expected_result, list(actual_result))


class TestPercentileStats(unittest.TestCase):
    def test_zero(self):
        stat = PercentileStats.new_empty(5)
        self.assertEqual(stat.url_total, 0)
        self.assertEqual(stat.min, 0)
        self.assertEqual(stat.max, 0)
        self.assertEqual(stat.url_total, 0)
        self.assertEqual(stat.percentile_id, 5)
        self.assertEqual(stat.avg, 0)

    def test_merge(self):
        stat = PercentileStats.new_empty(10)
        # avg 2017
        metrics = [15, 30, 23, 49, 10239]
        for m in metrics:
            stat.merge(m)

        self.assertEqual(stat.min, 15)
        self.assertEqual(stat.max, 10239)
        self.assertEqual(stat.avg, 2071)
        self.assertEqual(stat.url_total, 5)
        self.assertEqual(stat.metric_total, 10356)

    def test_to_dict(self):
        stat = PercentileStats.new_empty(10)
        stat.merge(1)
        stat.merge(5)
        result = stat.to_dict()
        expected = {
            'id': 10,
            'metric_total': 6,
            'url_total': 2,
            'avg': 3,
            'min': 1,
            'max': 5,
        }
        self.assertEqual(result, expected)


class TestComputePercentile(unittest.TestCase):
    def setUp(self):
        self.urlids_stream = iter([
            (1, "http", "foo.com", "/"),
            (2, "http", "foo.com", "/index.html"),
            (3, "http", "foo.com", "/bar"),
            (4, "http", "foo.com", "/baz"),
            (5, "http", "foo.com", "/qux"),
            (6, "http", "foo.com", "/barbar"),
        ])
        self.inredirections_stream = iter([
        ])
        self.max_crawled_urlid = 6

    def test_nominal_case(self):
        inlinks_count_stream = iter([
            (1, ["follow"], 10,  10),
            (2, ["follow"], 2, 2),
            (3, ["follow"], 1, 1),
            (4, ["follow"], 6, 6),
            (5, ["follow"], 5, 5),
            (6, ["follow"], 8, 8)
        ])
        nb_elements = 3
        actual_result = compute_quantiles(
            self.urlids_stream,
            inlinks_count_stream,
            self.inredirections_stream,
            self.max_crawled_urlid,
            nb_elements
        )
        expected_result = [
            (1, 2, 10),
            (2, 0, 2),
            (3, 0, 1),
            (4, 1, 6),
            (5, 1, 5),
            (6, 2, 8)
        ]
        self.assertEqual(expected_result, list(actual_result))

    def test_secondary_sort_criterion(self):
        #all urls have the same number of inlinks
        inlinks_count_stream = iter([
            (1, ["follow"], 4, 4),
            (2, ["follow"], 4, 4),
            (3, ["follow"], 4, 4),
            (4, ["follow"], 4, 4),
            (5, ["follow"], 4, 4),
            (6, ["follow"], 4, 4)
        ])
        nb_elements = 3
        actual_result = compute_quantiles(
            self.urlids_stream,
            inlinks_count_stream,
            self.inredirections_stream,
            self.max_crawled_urlid,
            nb_elements
        )
        #the urls are sorted by decreasing urlids
        expected_result = [
            (1, 2, 4),
            (2, 2, 4),
            (3, 1, 4),
            (4, 1, 4),
            (5, 0, 4),
            (6, 0, 4)
        ]
        self.assertEqual(expected_result, list(actual_result))


class TestPercentileStatsComputation(unittest.TestCase):
    def _generate_fixture(self):
        """Randomly generate test fixture
        """
        # percentiles
        pid_size = random.randint(5, 10)
        pids = range(0, pid_size)
        # length per percentile group
        p_length = [random.randint(5, 15) for _ in xrange(0, pid_size)]
        # metrics
        metrics = [
            [random.randint(0, 100) for _ in xrange(0, p_length[i])]
            for i in pids
        ]
        # urls (not important, all set as 1)
        urls = [
            [1] * p_length[i] for i in xrange(0, pid_size)
        ]
        # fixture
        fixture = [
            zip(urls[i], [pids[i]] * p_length[i], metrics[i])
            for i in xrange(0, pid_size)
        ]
        # flatten it
        fixture = [item for sublist in fixture for item in sublist]
        random.shuffle(fixture)

        self.metrics = metrics
        self.fixture = fixture
        self.p_length = p_length

    def setUp(self):
        self._generate_fixture()

    def test_random(self):
        result = compute_percentile_stats(self.fixture)
        self.assertEqual(len(result), len(self.p_length))

        for stat in result:
            pid = stat.percentile_id
            try:
                self.assertEqual(stat.min, min(self.metrics[pid]))
                self.assertEqual(stat.max, max(self.metrics[pid]))
                self.assertEqual(stat.metric_total, sum(self.metrics[pid]))
                self.assertEqual(stat.url_total, self.p_length[pid])
                self.assertEqual(stat.avg, sum(self.metrics[pid]) / self.p_length[pid])
            except AssertionError:
                print "Fixture:\n {}".format(self.fixture)
                raise

    def test_harness(self):
        fixture = [
            (1, 1, 15),
            (2, 2, 40),
            (3, 2, 2),
            (4, 1, 9),
            (5, 1, 17),
        ]
        result = compute_percentile_stats(fixture)
        self.assertEqual(len(result), 2)

        stat1 = result[0]
        self.assertEqual(stat1.min, 9)
        self.assertEqual(stat1.max, 17)
        self.assertEqual(stat1.url_total, 3)
        self.assertEqual(stat1.metric_total, 41)
        self.assertEqual(stat1.avg, 41 / 3)

        stat1 = result[1]
        self.assertEqual(stat1.min, 2)
        self.assertEqual(stat1.max, 40)
        self.assertEqual(stat1.url_total, 2)
        self.assertEqual(stat1.metric_total, 42)
        self.assertEqual(stat1.avg, 21)
