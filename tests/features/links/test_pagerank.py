import unittest
import StringIO
import tempfile
import os

from cdf.features.links.pagerank import FileBackedLinkGraph


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