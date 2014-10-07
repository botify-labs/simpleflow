import unittest
import random

from cdf.features.links.percentiles import (
    generate_follow_inlinks_stream,
    PercentileStats,
    compute_percentile_stats
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

        self.assertEqual(stat.min, min(metrics))
        self.assertEqual(stat.max, max(metrics))
        self.assertEqual(stat.avg, sum(metrics) / len(metrics))
        self.assertEqual(stat.url_total, len(metrics))
        self.assertEqual(stat.metric_total, sum(metrics))

    def test_to_dict(self):
        stat = PercentileStats.new_empty(10)
        stat.merge(1)
        result = stat.to_dict()
        expected = {
            'id': 10,
            'metric_total': 1,
            'url_total': 1,
            'avg': 1,
            'min': 1,
            'max': 1,
        }
        self.assertEqual(result, expected)


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

    def test_harness(self):
        result = compute_percentile_stats(self.fixture)
        self.assertEqual(len(result), len(self.p_length))

        for stat in result:
            pid = stat.percentile_id
            self.assertEqual(stat.min, min(self.metrics[pid]))
            self.assertEqual(stat.max, max(self.metrics[pid]))
            self.assertEqual(stat.metric_total, sum(self.metrics[pid]))
            self.assertEqual(stat.url_total, self.p_length[pid])
            self.assertEqual(stat.avg, sum(self.metrics[pid]) / self.p_length[pid])
