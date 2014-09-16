from itertools import groupby, ifilter, imap
import heapq
from cdf.features.links.helpers.predicates import (
    is_link,
    is_link_internal,
    is_follow_link
)
from cdf.utils.url import get_domain, get_second_level_domain
from cdf.utils.external_sort import external_sort
from cdf.features.links.streams import OutlinksRawStreamDef


def filter_external_outlinks(outlinks):
    """Filter outlinks stream for external, <a> links

    :param outlinks: stream of OutLinksRawStreamDef
    :return: external, <a> outlinks stream
    """
    mask_idx = OutlinksRawStreamDef.field_idx('bitmask')
    dest_idx = OutlinksRawStreamDef.field_idx('dst_url_id')
    type_idx = OutlinksRawStreamDef.field_idx('link_type')
    # filter <a> links
    filtered = ifilter(
        lambda l: is_link(l[type_idx]),
        outlinks
    )
    # filter external outgoing links
    filtered = ifilter(
        lambda l: not is_link_internal(
            l[mask_idx], l[dest_idx], is_bitmask=True),
        filtered
    )
    return filtered


def _group_links(link_stream, key):
    """A helper function to group elements of a outlink stream
    according to a generic criterion.
    It returns tuples (key_value, corresponding links)
    :param link_stream: the input outlink stream from OutlinksRawStreamDef
                        (should contains only outlinks,
                        no inlinks, no canonical)
    :param link_stream: iterable
    """
    #sort links by key function
    link_stream = external_sort(link_stream, key=key)
    #group by key function
    for key_value, link_group in groupby(link_stream, key=key):
        yield key_value, list(link_group)


def group_links_by_domain(external_outlinks):
    """Given a stream of *external* outlinks, groups them by out domain
    and generates pairs (domain, link_list)
    :param link_stream: the input outlink stream from OutlinksRawStreamDef
                        (should contains only outlinks,
                        no inlinks, no canonical)

    :param link_stream: iterable
    """
    external_url_idx = OutlinksRawStreamDef.field_idx("external_url")
    key = lambda x: get_domain(x[external_url_idx])
    for key_value, link_group in _group_links(external_outlinks, key):
        yield key_value, link_group


def group_links_by_second_level_domain(external_outlinks):
    """Given a stream of *external* outlinks,
    groups them by second level out domain
    and generates pairs (domain, link_list).
    For instance if the external_outlinks are:
      (0, "a", 0, -1, "http://foo.com/bar.html")
      (0, "a", 0, -1, "http://bar.com/image.jpg")
      (4, "a", 0, -1, "http://bar.foo.com/baz.html")
    the result will be
      ("bar.com", [(0, "a", 0, -1, "http://bar.com/image.jpg")])
      ("foo.com", [(0, "a", 0, -1, "http://foo.com/bar.html"),
                   (4, "a", 0, -1, "http://bar.foo.com/baz.html")])

    :param link_stream: the input outlink stream from OutlinksRawStreamDef
                        (should contains only outlinks,
                        no inlinks, no canonical)
    :param link_stream: iterable
    """
    external_url_idx = OutlinksRawStreamDef.field_idx("external_url")
    key = lambda x: get_second_level_domain(x[external_url_idx])
    for key_value, link_group in _group_links(external_outlinks, key):
        yield key_value, link_group


def count_unique_links(external_outlinks):
    """Count the number of unique links in a set of external outlinks.
    i.e. if a link to B occurs twice in page A, it is counted only once.
    :param external_outlinks: the input stream of external outlinks
                              (based on OutlinksRawStreamDef)
    :type external_outlinks: iterable
    :rtype: int
    """
    #remove duplicate links
    id_index = OutlinksRawStreamDef.field_idx("id")
    external_url_index = OutlinksRawStreamDef.field_idx("external_url")
    external_outlinks = imap(
        lambda x: (x[id_index], x[external_url_index]),
        external_outlinks
    )
    result = len(set(external_outlinks))
    return result


def _compute_top_domains(external_outlinks, n, key):
    """A helper function to compute the top n domains given a custom criterion.
    For each destination domain the function counts the number of unique follow
    links that points to it and use this number to select the top n domains.
    The method returns a list of tuple (nb unique follow links, domain)
    Elements are sorted by decreasing number of unique follow links
    :param external_outlinks: the input stream of external outlinks
                              (based on OutlinksRawStreamDef)
    :type external_outlinks: iterable
    :param n: the maximum number of domains we want to return
    :type n: int
    :param key: the function that extracts the domain from an entry from
                external_outlinks.
    :type key: func
    :rtype: list
    """
    heap = []
    bitmask_index = OutlinksRawStreamDef.field_idx("bitmask")
    for domain, link_group in _group_links(external_outlinks, key):

        #compute number of unique follow links
        external_follow_outlinks = ifilter(
            lambda x: is_follow_link(x[bitmask_index], is_bitmask=True),
            link_group
        )
        nb_unique_follow_links = count_unique_links(external_follow_outlinks)

        if nb_unique_follow_links == 0:
            #we don't want to return domain with 0 occurrences.
            continue
        if len(heap) < n:
            heapq.heappush(heap, (nb_unique_follow_links, domain))
        else:
            heapq.heappushpop(heap, (nb_unique_follow_links, domain))
    #back to a list
    result = []
    while len(heap) != 0:
        nb_unique_follow_links, domain = heap.pop()
        result.append((nb_unique_follow_links, domain))
    return result


def compute_top_domains(external_outlinks, n):
    """A helper function to compute the top n domains.
    For each destination domain the function counts the number of unique follow
    links that points to it and use this number to select the top n domains.
    The method returns a list of tuple (nb unique follow links, domain)
    Elements are sorted by decreasing number of unique follow links
    :param external_outlinks: the input stream of external outlinks
                              (based on OutlinksRawStreamDef)
    :type external_outlinks: iterable
    :param n: the maximum number of domains we want to return
    :type n: int
    :param key: the function that extracts the domain from an entry from
                external_outlinks.
    :type key: func
    :rtype: list
    """
    external_url_idx = OutlinksRawStreamDef.field_idx("external_url")
    key = lambda x: get_domain(x[external_url_idx])
    return _compute_top_domains(external_outlinks, n, key)


def compute_top_second_level_domains(external_outlinks, n):
    """A helper function to compute the top n second level domains.
    The method is very similar to "compute_top_n_domains()" but it consider
    "doctissimo.fr" and "forum.doctissimo.fr" as the same domain
    while "compute_top_n_domains()" consider them as different.
    :param external_outlinks: the input stream of external outlinks
                              (based on OutlinksRawStreamDef)
    :type external_outlinks: iterable
    :param n: the maximum number of domains we want to return
    :type n: int
    :param key: the function that extracts the domain from an entry from
                external_outlinks.
    :type key: func
    :rtype: list
    """
    external_url_idx = OutlinksRawStreamDef.field_idx("external_url")
    key = lambda x: get_second_level_domain(x[external_url_idx])
    return _compute_top_domains(external_outlinks, n, key)
