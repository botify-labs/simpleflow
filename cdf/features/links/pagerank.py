"""Module for internal page rank computation

For the algorithm and its meaning, please check
    http://www.mmds.org/mmds/v2.1/ch05-linkanalysis1.pdf

The module has the following abstractions/concepts:
    - LinkGraph
        A LinkGraph represents the input graph of the internal page rank
        computation. We can iterate over a LinkGraph for all its nodes
        with its outgoing edges.
    - NodeIdMapping
        In the dataset nodes can have arbitrary identifier. For example
        in `urllinks`, node id starts from 1 and are not consecutive (with
        jumps). A NodeIdMapping will be applied to maintain a set of
        internal node ids that simplifies the algorithm implementation.
    - Page Rank algorithm
        The actual page rank algorithm takes an input LinkGraph and applies
        the algorithm. The final result is a compact numpy vector.
    - Pre-processing functions
        Stream filters, predicate functions
    - Post-processing functions
    - Virtual links
        Links that go to external pages, to non-crawled urls or blocked by
        robots.txt are classed as virtual links. They do not participate
        in the page rank computation. But we'll use the page rank result
        and the virtual links data to 'interpret' the amount of page rank
        that goes into these 3 categories of pages.

Conceptually, the internal page rank computation is:
  1. filter links dataset, keep only page rank eligible links
  2. separate normal links with virtual links
  3. use normal links to construct the LinkGraph
  4. launch the algorithm on the LinkGraph
  5. interpret the virtual links using page rank result
  6. post-process page rank result
  7. upload results
"""

import abc
from collections import namedtuple, Iterable
import itertools
from operator import itemgetter
import marshal
import numpy as np
import logging

from cdf.core.streams.base import StreamDefBase
from cdf.features.links.helpers.predicates import (
    is_follow_link,
    is_external_link,
    is_robots_blocked
)

logger = logging.getLogger(__name__)

EXT_VIR = 0
ROBOTS_VIR = 1
NOT_CRAWLED_VIR = 2


# Parameters for page rank
#   - `damping`: the probability that we follow a link of the page,
#       instead of teleporting.
#   - `epsilon`: the convergence condition (error measure of two page
#       rank vectors)
#   - `nb_iterations`: max number of iterations.
PageRankParams = namedtuple(
    'PageRankParams', ['damping', 'epsilon', 'nb_iterations'])
DEFAULT_PR_PARAM = PageRankParams(0.85, 0.0001, 100)


class NodeIdMapping(object):
    """Interface for node id mapping"""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_internal_id(self, ext_id):
        """Get the internal id corresponding to the external id"""
        raise NotImplemented

    @abc.abstractmethod
    def get_external_id(self, int_id):
        """Get the external id corresponding to the internal id"""
        raise NotImplemented

    @abc.abstractmethod
    def get_node_count(self):
        """Get the node count"""
        raise NotImplemented


class DictMapping(NodeIdMapping):
    """Node id mapping backed by a dict and an numpy array"""
    def __init__(self, id_stream):
        """Initialize this mapping by an external node id stream"""
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
    """A stream def for edge list file

    **Not used for the moment**
    """
    FILE = 'edgelist'
    HEADERS = (
        ('src', int),
        ('dst', int)
    )


class LinkGraph(Iterable):
    """Abstract graph representation"""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def iter_adjacency_list(self):
        """Returns a generator over the graph
        :returns: (src, out-degree, dests list)
        :rtype: iterator
        """
        raise NotImplemented

    def __iter__(self):
        return self.iter_adjacency_list()


class FileBackedLinkGraph(LinkGraph):
    """A LinkGraph impl backed by a on-disk file"""
    @classmethod
    def from_edge_list_file(cls, edge_list_file, graph_path,
                            node_mapping_cls=DictMapping):
        """Parse an edge list file into graph

        Outgoing edges of the same node are supposed to be consecutive

        **Not used for the moment, the graph file is directed constructed
        in the page rank task to avoid generate a edge list file**

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
        with open(self.path, 'rb') as graph_file:
            while True:
                try:
                    yield marshal.load(graph_file)
                except EOFError:
                    break


def compute_page_rank(graph, params=DEFAULT_PR_PARAM):
    """Compute the page rank vector for a given LinkGraph

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
            weight = src[i] / od
            for j in links:
                dst[j] = dst[j] + weight

        dst *= params.damping

        # with dead-ends, re-normalize to 1
        dst += (1.0 - dst.sum()) / node_count

        residual = np.linalg.norm(
            (src / np.linalg.norm(src)) - (dst / np.linalg.norm(dst)),
            ord=1
        )
        logger.info("%d iteration with residual %s", iter_count, str(residual))

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
    """Filter that allows only page rank eligible links to pass through

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


def get_bucket_size(num_pages):
    """Get the normalization bucket sizes, given the total
    number of pages

    :param num_pages: total number of pages in page rank computation
    :type num_pages: int
    :return: a list of n number indicating the bucket size of each
        normalized page rank number
        For > 1023 pages, there'll be 9 numbers
        For < 1023 pages, the element number depends on page number
    :rtype: list
    """
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
        while total < num_pages:
            size = pow(2, c)
            result.append(size)
            total += size
            c += 1
        return result[:-1]


# TODO same pr value should have the same normalized pr
def process_pr_result(pr_kv_list):
    """Post process the raw page rank result
        - assign a total rank for each page
        - attribute a normalized page rank for each page
        - sorted the final result by url id

    :param pr_kv_list:
    :return: final result list, (urlid, rank, pr_value, normalized_pr)
    :rtype: list
    """
    pr_sorted = sorted(pr_kv_list, key=itemgetter(1), reverse=True)

    with_ranks = []
    r = 1
    for k, g in itertools.groupby(pr_sorted, key=itemgetter(1)):
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


def process_virtual_result(virtuals_file, pr):
    """Helper function that process virtual links

    Assume that the `virtuals_file` contains marshalled data.

    :param virtuals_file: file containing virtual links data
    :type virtuals_file: file
    :param pr: raw page rank vector
    :type pr: numpy.array | list
    :returns: virtual page result
    :rtype: dict
    """
    virtuals_result = {
        EXT_VIR: 0.0,
        ROBOTS_VIR: 0.0,
        NOT_CRAWLED_VIR: 0.0,
    }

    while True:
        try:
            # note that `src` is internal id
            src, od, virtuals = marshal.load(virtuals_file)
            contrib = pr[src] / od

            exts = virtuals[EXT_VIR]
            robots = virtuals[ROBOTS_VIR]
            non_crawls = virtuals[NOT_CRAWLED_VIR]

            virtuals_result[EXT_VIR] += contrib * exts
            virtuals_result[ROBOTS_VIR] += contrib * robots
            virtuals_result[NOT_CRAWLED_VIR] += contrib * non_crawls
        except EOFError:
            break

    return virtuals_result