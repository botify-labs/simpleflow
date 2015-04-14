import unittest
import StringIO
import tempfile
import os

import numpy as np

from cdf.features.links.pagerank import (
    FileBackedLinkGraph,
    compute_page_rank
)


class TestGraph(unittest.TestCase):
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

        graph = FileBackedLinkGraph.from_edge_list_file(f, self.output)
        result = list(graph.iter_adjacency_list())
        expected = [
            (1, 3, [2, 2, 3]),
            (2, 1, [3])
        ]
        self.assertEqual(result, expected)
        self.assertEqual(graph.node_count, 3)


class MockGraph(object):
    """Test graph for page rank

    1       2
    1       6
    2       3
    2       4
    3       4
    3       5
    3       6
    4       1
    6       1
    """
    node_count = 6

    def __iter__(self):
        return iter([
            (1, 2, [2, 6]),
            (2, 2, [3, 4]),
            (3, 3, [4, 5, 6]),
            (4, 1, [1]),
            (6, 1, [1])
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

