"""Module for internal page rank computation

*** PROTOTYPE NOT TO BE USED IN PRODUCTION ***
"""

import abc
import itertools
from operator import itemgetter
import networkx as nx
import marshal

from cdf.core.streams.base import StreamDefBase
from cdf.features.links.helpers.predicates import (
    is_follow_link,
    is_external_link,
    is_robots_blocked
)


EXT_VIR = -2
ROBOTS_VIR = -3
NOT_CRAWLED_VIR = -4


class EdgeListStreamDef(StreamDefBase):
    FILE = 'edgelist'
    HEADERS = (
        ('src', int),
        ('dst', int)
    )


class LinkGraph(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def iter_adjacency_list(self):
        raise NotImplemented


class FileBackedLinkGraph(LinkGraph):
    @classmethod
    def from_edge_list_file(cls, edge_list_file, graph_path):
        """Parse an edge list file into graph

        Outgoing edges of the same node are supposed to be consecutive

        :param edge_list_file: edge list file
        :type edge_list_file: file
        :param graph_path: on-disk graph file's path
        :type graph_path: str
        :returns: graph object
        :rtype: FileBackedLinkGraph
        """
        nodes = set()
        with open(graph_path, 'wb') as graph_file:
            stream = EdgeListStreamDef.load_file(edge_list_file)
            for k, g in itertools.groupby(stream, key=itemgetter(0)):
                g = [d for s, d in g]
                nodes.add(k)
                for d in g:
                    nodes.add(d)
                s = marshal.dumps((k, len(g), g))
                graph_file.write(s)
                graph_file.write('\n')

        return cls(path=graph_path, node_count=len(nodes))

    def __init__(self, path, node_count):
        self.path = path
        self.node_count = node_count

    def iter_adjacency_list(self):
        """Returns a generator over the graph
        :returns: (src, out-degree, dests list)
        :rtype: iterator
        """
        with open(self.path, 'rb') as graph_file:
            for l in graph_file:
                yield marshal.loads(l)


def virtuals_filter(links, max_crawled_id):
    for id, type, mask, dst, _ in links:
        if id == dst:
            continue
        if type.startswith('c'):
            continue
        if not is_follow_link(mask, True):
            continue

        if is_external_link(mask):
            dst = EXT_VIR
        elif is_robots_blocked(mask):
            dst = ROBOTS_VIR
        elif dst > max_crawled_id:
            dst = NOT_CRAWLED_VIR
        else:
            continue

        yield id, dst


def pagerank_filter(links, max_crawled_id=-1, virtual_pages=False):
    for id, type, mask, dst, ext_url in links:
        if not virtual_pages:
            # skip external, robots blocked and non-crawled pages
            # if virtual pages are not activated
            if dst < 0:
                continue
            if is_robots_blocked(mask):
                continue
            if dst > max_crawled_id:
                continue

        if id == dst:
            continue
        if type.startswith('c'):
            continue
        if not is_follow_link(mask, True):
            continue

        if virtual_pages:
            if is_external_link(mask):
                dst = EXT_VIR
            elif is_robots_blocked(mask):
                dst = ROBOTS_VIR
            elif dst > max_crawled_id:
                dst = NOT_CRAWLED_VIR
            # else, not a virtual page

        yield id, dst


def get_bucket_size(num_pages):
    result = []
    if num_pages > 1023:
        c = num_pages
        for i in range(10, 0, -1):
            c /= 2
            result.append(c)
        result.reverse()
        return result[:9]
    else:
        c = 0
        total = 0
        while total <= num_pages:
            size = pow(2, c)
            result.append(size)
            total += size
            c += 1
        return result[:-1]


def compute_page_rank(links):
    dg = nx.MultiDiGraph()
    outdegrees = {}
    for k, g in itertools.groupby(links, key=lambda x: x[0]):
        g = list(g)
        outdegrees[k] = len(g)
        for _, d in g:
            dg.add_edge(k, d)

    return nx.pagerank_scipy(dg), outdegrees


def process_pr_result(pr_dict):
    # sorted on page rank value
    pr = sorted([(k, v) for k, v in pr_dict.iteritems() if k > 0],
                key=lambda x: x[1], reverse=True)

    with_ranks = []
    r = 1
    for k, g in itertools.groupby(pr, key=lambda x: x[1]):
        c = 0
        for k, v in g:
            c += 1
            with_ranks.append([k, r, v])
        r += c

    buckets = get_bucket_size(len(with_ranks))
    i = 0
    rank = 10
    for s in buckets:
        for _ in range(0, s):
            with_ranks[i].append(rank)
            i += 1
        rank -= 1

    # attribute the last rank for the rest of the urls
    while i < len(with_ranks):
        with_ranks[i].append(rank)
        i += 1

    # sorted on url_id
    return sorted(with_ranks, key=lambda x: x[0])