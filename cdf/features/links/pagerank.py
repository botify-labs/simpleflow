"""Module for internal page rank computation

*** PROTOTYPE NOT TO BE USED IN PRODUCTION ***
"""

import abc
from collections import namedtuple
import itertools
from operator import itemgetter
import networkx as nx
import marshal
import numpy as np

from cdf.core.streams.base import StreamDefBase
from cdf.features.links.helpers.predicates import (
    is_follow_link,
    is_external_link,
    is_robots_blocked
)


EXT_VIR = 0
ROBOTS_VIR = 1
NOT_CRAWLED_VIR = 2


PageRankParams = namedtuple(
    'PageRankParams', ['damping', 'epsilon', 'nb_iterations'])
DEFAULT_PR_PARAM = PageRankParams(0.85, 0.001, 100)


class NodeIdMapping(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_internal_id(self, ext_id):
        raise NotImplemented

    @abc.abstractmethod
    def get_external_id(self, int_id):
        raise NotImplemented

    @abc.abstractmethod
    def get_node_count(self):
        raise NotImplemented


class DictMapping(NodeIdMapping):
    def __init__(self, id_stream):
        nodes = set()
        for n in id_stream:
            nodes.add(n)
        nodes = np.array(list(nodes))
        nodes.sort()

        self.int_ext_array = nodes
        self.ext_int_dict = {
            e: i for i, e in enumerate(self.int_ext_array)
        }

    def get_internal_id(self, ext_id):
        return self.ext_int_dict[ext_id]

    def get_external_id(self, int_id):
        return self.int_ext_array[int_id]

    def get_node_count(self):
        return len(self.int_ext_array)


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

    def __iter__(self):
        return self.iter_adjacency_list()


class FileBackedLinkGraph(LinkGraph):
    @classmethod
    def from_edge_list_file(cls, edge_list_file, graph_path,
                            node_mapping_cls=DictMapping):
        """Parse an edge list file into graph

        Outgoing edges of the same node are supposed to be consecutive

        :param edge_list_file: edge list file
        :type edge_list_file: file
        :param graph_path: on-disk graph file's path
        :type graph_path: str
        :returns: graph object
        :rtype: FileBackedLinkGraph
        """
        # first pass for node id conversion
        stream = EdgeListStreamDef.load_file(edge_list_file)

        def get_id_stream(edge_list_stream):
            for s, d in edge_list_stream:
                yield s
                yield d

        node_mapping = node_mapping_cls(get_id_stream(stream))

        with open(graph_path, 'wb') as graph_file:
            edge_list_file.seek(0)
            stream = EdgeListStreamDef.load_file(edge_list_file)
            for k, g in itertools.groupby(stream, key=itemgetter(0)):
                k = node_mapping.get_internal_id(k)
                g = [node_mapping.get_internal_id(d) for s, d in g]
                marshal.dump((k, len(g), g), graph_file)
        edge_list_file.close()

        return cls(
            path=graph_path,
            node_mapping=node_mapping
        )

    def __init__(self, path, node_mapping):
        self.path = path
        self.node_count = node_mapping.get_node_count()
        self.node_mapping = node_mapping

    def iter_adjacency_list(self):
        """Returns a generator over the graph
        :returns: (src, out-degree, dests list)
        :rtype: iterator
        """
        with open(self.path, 'rb') as graph_file:
            while True:
                try:
                    yield marshal.load(graph_file)
                except EOFError:
                    break


def compute_page_rank(graph, params=DEFAULT_PR_PARAM):
    """Compute the page rank vector for a given graph

    :param graph: the link graph
    :type graph: LinkGraph
    :param params: page rank algorithm params
    :type params: PageRankParams
    :return: page rank vector
    :rtype: numpy.array
    """
    node_count = graph.node_count

    residual = float('inf')
    src = np.repeat(1.0 / node_count, node_count)
    iter_count = 0

    while (residual > params.epsilon
           and iter_count < params.nb_iterations):
        # TODO reuse buffer ???
        dst = np.zeros(node_count)

        for i, od, links in graph:
            # TODO node id translation
            weight = src[i] / od
            for j in links:
                dst[j] = dst[j] + weight

        dst *= params.damping

        # with dead-ends, re-normalize to 1
        dst += (1.0 - dst.sum()) / node_count

        residual = np.linalg.norm(src - dst)
        src = dst
        iter_count += 1

    return src


def is_virtual_page(src, mask, dst, max_crawled_id):
    """Predicate to check if a link is virtual

    :param src: link src
    :type src: int
    :param mask: link mask
    :type mask: int
    :param dst: link dest
    :type dst: int
    :param max_crawled_id: max crawled url id
    :type max_crawled_id: int
    :return: a link tuple if it's a virtual page, otherwise False
    """
    if dst > max_crawled_id:
        return src, NOT_CRAWLED_VIR
    if is_external_link(mask):
        return src, EXT_VIR
    if is_robots_blocked(mask):
        return src, ROBOTS_VIR

    return False


def pagerank_filter(link):
    """Filter out the links that does not fall into the scope of
    page rank computation

    :param link: a link tuple
    """
    src, type, mask, dst, _ = link
    return (
        src != dst and
        type[0] != 'c' and
        is_follow_link(mask, True) and
        # special case, need to know why this happens @stan
        # equivalent to `not (not is_external_lin(mask) and dst < 0)`
        (dst > 0 or is_external_link(mask))
    )


def group_links(links_stream, max_crawled_id):
    """Group links into page rank internal format for further processing

        - normal links are stored in a list: [1, 2, 5, 10, ...]
        - virtuals are stored by count: [15, 2, 20] means 15 links to external
            2 links to robots blocked and 20 links to non-crawled pages

    :param links_stream: link stream
    :type links_stream: iterator
    :param max_crawled_id: max crawled url id
    :type max_crawled_id: int
    :return: (src, out_degree, normals, virtuals)
    """
    for src, g in itertools.groupby(links_stream, key=itemgetter(0)):
        od_count = 0
        virtuals = None
        normals = []
        for _, _, mask, dst, _ in g:
            od_count += 1
            v = is_virtual_page(src, mask, dst, max_crawled_id)
            if v:
                if not virtuals:
                    virtuals = [0, 0, 0]
                _, d = v
                virtuals[d] += 1
            else:
                normals.append(dst)
        yield (src, od_count, normals, virtuals)


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


def pagerank_filter_nx(links, max_crawled_id=-1, virtual_pages=False):
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


def compute_page_rank_nx(links):
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