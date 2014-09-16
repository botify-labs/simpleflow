from itertools import groupby
from cdf.utils.url import get_domain, get_top_level_domain
from cdf.utils.external_sort import external_sort
from cdf.features.links.streams import OutlinksStreamDef


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


def group_links_by_domain(link_stream):
    """Given a stream of outlinks, groups them by out domain
    and generates pairs (domain, link_list)
    :param link_stream: the input outlink stream from OutlinksStreamDef
                        (should contains only outlinks,
                        no inlinks, no canonical)

    :param link_stream: iterable
    """
    external_url_idx = OutlinksStreamDef.field_idx("external_url")
    key = lambda x: get_domain(x[external_url_idx])
    for key_value, link_group in _group_links(link_stream, key):
        yield key_value, link_group


def group_links_by_top_level_domain(link_stream):
    """Given a stream of outlinks, groups them by top level out domain
    and generates pairs (domain, link_list)
    :param link_stream: the input outlink stream from OutlinksStreamDef
                        (should contains only outlinks,
                        no inlinks, no canonical)
    :param link_stream: iterable
    """
    external_url_idx = OutlinksStreamDef.field_idx("external_url")
    key = lambda x: get_top_level_domain(x[external_url_idx])
    for key_value, link_group in _group_links(link_stream, key):
        yield key_value, link_group
