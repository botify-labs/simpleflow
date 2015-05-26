import unittest
import StringIO
import tempfile
import os

import numpy as np

from cdf.features.links.pagerank import (
    FileBackedLinkGraph,
    compute_page_rank,
    NodeIdMapping,
    DictMapping,
    is_virtual_page,
    pagerank_filter,
    group_links,
    get_bucket_size,
    process_pr_result,
    EXT_VIR, NOT_CRAWLED_VIR, ROBOTS_VIR
)


class TestDataPreparation(unittest.TestCase):
    def test_virtual_predicate(self):
        self.assertEqual(is_virtual_page(0, 100, 50), NOT_CRAWLED_VIR)
        self.assertEqual(is_virtual_page(8, 2, 50), EXT_VIR)
        self.assertEqual(is_virtual_page(4, 2, 50), ROBOTS_VIR)

        self.assertFalse(is_virtual_page(0, 2, 50))
        self.assertFalse(is_virtual_page(15, 0, 50), {15, 32})

    def test_filtering_self_link(self):
        self.assertFalse(pagerank_filter((1, 'a', 0, 1, '')))

    def test_filtering_nofollow(self):
        self.assertFalse(pagerank_filter((1, 'a', 1, 5, '')))

    def test_filtering_canonical(self):
        self.assertFalse(pagerank_filter((1, 'canonical', 0, 5, '')))

    # links that should not be crawled @stan
    def test_filtering_image_link(self):
        self.assertFalse(pagerank_filter((1, 'a', 0, -1, 'abc')))

    def test_filtering_oks(self):
        self.assertTrue(pagerank_filter((1, 'a', 0, 2, '')))
        self.assertTrue(pagerank_filter((1, 'a', 8, -1, 'abcd')))
        self.assertTrue(pagerank_filter((1, 'r301', 0, 3, '')))

    def test_grouping_normals(self):
        s = iter([
            (1, 'a', 0, 2, ''),
            (1, 'a', 0, 2, ''),
            (1, 'a', 0, 5, '')
        ])
        result = list(group_links(
            s, max_crawled_id=15, extra_non_crawls=set()))
        expected = [
            (1, 3, [2, 2, 5], None)
        ]
        self.assertEqual(result, expected)

    def test_grouping_virtuals(self):
        s = iter([
            (1, 'a', 8, -1, ''),
            (1, 'a', 8, -1, ''),
            (1, 'a', 8, -1, ''),
            (1, 'a', 0, 200, ''),
            (1, 'a', 4, 5, '')
        ])
        result = list(group_links(
            s, max_crawled_id=15, extra_non_crawls=set()))
        expected = [
            (1, 5, [], [3, 1, 1])
        ]
        self.assertEqual(result, expected)


class TestPageRankPostProcessing(unittest.TestCase):
    def test_process_result_simple(self):
        pr = [(1, 5), (2, 7), (5, 8), (6, 100),
              (10, 1), (15, 2), (16, 4), (17, 3)]
        result = list(process_pr_result(pr))

        expected = [
            (1, 4, 5, 8.6), (2, 3, 7, 8.8), (5, 2, 8, 8.9), (6, 1, 100, 10.0),
            (10, 8, 1, 8.0), (15, 7, 2, 8.3), (16, 5, 4, 8.6), (17, 6, 3, 8.4)
        ]
        self.assertEqual(result, expected)

    def test_process_result_same_value(self):
        pr = [(1, 7), (2, 7), (5, 9), (6, 11),
              (10, 1), (15, 7), (16, 4), (17, 3)]
        result = list(process_pr_result(pr))
        expected = [
            (1, 3, 7, 9.8), (2, 3, 7, 9.8), (5, 2, 9, 9.9), (6, 1, 11, 10.0),
            (10, 8, 1, 8.9), (15, 3, 7, 9.8), (16, 6, 4, 9.5), (17, 7, 3, 9.4)
        ]
        self.assertEqual(result, expected)


class IdentityNodeMapping(NodeIdMapping):
    def __init__(self, id_stream):
        self.nodes = set()
        for n in id_stream:
            self.nodes.add(n)

    def get_internal_id(self, ext_id):
        return ext_id

    def get_external_id(self, int_id):
        return int_id

    def get_node_count(self):
        return len(self.nodes)


class TestLinkGraph(unittest.TestCase):
    def setUp(self):
        self.output = ''

    def tearDown(self):
        if os.path.exists(self.output):
            os.remove(self.output)

    def test_parse_edge_list(self):
        f = StringIO.StringIO()
        f.write('1\t2\n')
        f.write('1\t2\n')
        f.write('1\t3\n')
        f.write('2\t3\n')
        f.seek(0)
        self.output = tempfile.mktemp('graph')

        graph = FileBackedLinkGraph.from_edge_list_file(
            f, self.output, IdentityNodeMapping
        )
        result = list(graph.iter_adjacency_list())
        expected = [
            (1, 3, [2, 2, 3]),
            (2, 1, [3])
        ]
        self.assertEqual(result, expected)
        self.assertEqual(graph.node_count, 3)


class TestNodeIdMapping(unittest.TestCase):
    def test_resolve(self):
        #   0   1   2   3   4
        #   1   2   4   5  19
        mapping = DictMapping(iter([19, 4, 5, 1, 2]))

        self.assertEqual(mapping.get_node_count(), 5)
        self.assertEqual(mapping.get_internal_id(19), 4)
        self.assertEqual(mapping.get_internal_id(1), 0)

        self.assertEqual(mapping.get_external_id(2), 4)
        self.assertEqual(mapping.get_external_id(4), 19)


class MockGraph(object):
    """Test graph for page rank

    0   1
    0   5
    1   2
    1   3
    2   3
    2   4
    2   5
    3   0
    5   0
    """
    def get_node_count(self):
        return 6

    def __iter__(self):
        return iter([
            (0, 2, [1, 5]),
            (1, 2, [2, 3]),
            (2, 3, [3, 4, 5]),
            (3, 1, [0]),
            (5, 1, [0])
        ])


class TestAlgorithm(unittest.TestCase):
    def test_graph(self):
        result = compute_page_rank(MockGraph())
        expected = np.array([
            0.3210154,
            0.1705440,
            0.1065908,
            0.1367922,
            0.0643121,
            0.2007454
        ])
        self.assertTrue(np.linalg.norm(result - expected) < 0.001)

