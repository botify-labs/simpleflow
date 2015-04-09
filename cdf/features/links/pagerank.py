"""Module for internal page rank computation

*** PROTOTYPE NOT TO BE USED IN PRODUCTION ***
"""


import itertools
import networkx as nx
from cdf.features.links.helpers.predicates import (
    is_follow_link,
    is_external_link,
    is_robots_blocked
)


EXT_VIR = -2
ROBOTS_VIR = -3
NOT_CRAWLED_VIR = -4


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