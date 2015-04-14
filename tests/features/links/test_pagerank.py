import unittest
import StringIO
import tempfile
import os

import numpy as np

from cdf.features.links.pagerank import (
    FileBackedLinkGraph,
    compute_page_rank,
    NodeIdMapping,
    DictMapping
)


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
    node_count = 6

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

