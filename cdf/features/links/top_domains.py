from itertools import groupby, ifilter
from cdf.features.links.helpers.predicates import (
    is_link,
    is_link_internal
)
from cdf.utils.url import get_domain, get_second_level_domain
from cdf.utils.external_sort import external_sort
from cdf.features.links.streams import OutlinksStreamDef


def filter_external_outlinks(outlinks):
    """Filter outlinks stream for external, <a> links

    :param outlinks: decoded outlinks stream
    :return: external, <a> outlinks stream
    """
    mask_idx = OutlinksStreamDef.field_idx('follow')
    dest_idx = OutlinksStreamDef.field_idx('dst_url_id')
    type_idx = OutlinksStreamDef.field_idx('link_type')
    # filter <a> links
    filtered = ifilter(
        lambda l: is_link(l[type_idx]),
        outlinks
    )
    # filter external outgoing links
    filtered = ifilter(
        lambda l: not is_link_internal(l[mask_idx], l[dest_idx]),
        filtered
    )
    return filtered


def _group_links(link_stream, key):
    """A helper function to group elements of a outlink stream
    according to a generic criterion.
    It returns tuples (key_value, corresponding links)
    :param link_stream: the input outlink stream from OutlinksStreamDef
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
    :param link_stream: the input outlink stream from OutlinksStreamDef
                        (should contains only outlinks,
                        no inlinks, no canonical)

    :param link_stream: iterable
    """
    external_url_idx = OutlinksStreamDef.field_idx("external_url")
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

    :param link_stream: the input outlink stream from OutlinksStreamDef
                        (should contains only outlinks,
                        no inlinks, no canonical)
    :param link_stream: iterable
    """
    external_url_idx = OutlinksStreamDef.field_idx("external_url")
    key = lambda x: get_second_level_domain(x[external_url_idx])
    for key_value, link_group in _group_links(external_outlinks, key):
        yield key_value, link_group
