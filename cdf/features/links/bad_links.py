from collections import Counter
from itertools import groupby

from cdf.features.main.streams import InfosStreamDef, CompliantUrlStreamDef
from cdf.features.links.streams import (
    OutlinksStreamDef,
    BadLinksStreamDef,
    LinksToNonCompliantStreamDef
)


def get_bad_links(stream_infos, stream_outlinks):
    """
    (url_src_id, url_dest_id, error_http_code)
    """
    # Resolve indexes
    http_code_idx = InfosStreamDef.field_idx('http_code')
    url_id_idx = InfosStreamDef.field_idx('id')
    dest_url_idx = OutlinksStreamDef.field_idx('dst_url_id')
    src_url_idx = OutlinksStreamDef.field_idx('id')
    link_type_idx = OutlinksStreamDef.field_idx('link_type')

    # Find all bad code pages
    # (url_id -> error_http_code)
    bad_code = {}
    for info in stream_infos:
        http_code = info[http_code_idx]
        if http_code >= 300:
            bad_code[info[url_id_idx]] = http_code

    # Iterator over outlinks and extract all normal links whose destination
    # is in the *bad_code* dict
    for outlink in stream_outlinks:
        dest = outlink[dest_url_idx]
        link_type = outlink[link_type_idx]
        if link_type == 'a' and dest in bad_code:
            yield (outlink[src_url_idx], dest, bad_code[dest])


def get_links_to_non_compliant_urls(stream_compliant, stream_outlinks):
    """Compute a stream of outlinks to non compliant urls.
    The result stream is based on LinksToNonCompliantStreamDef.
    :param stream_compliant: a stream of compliant urls (based on CompliantUrlStreamDef)
    :type stream_compliant: iterable
    :param stream_outlinks: a stream of outlinks (based on OutlinksStreamDef)
    :type stream_outlinks: iterable
    :returns: iterable - the stream of outlinks to non compliant urls.
    """
    # Resolve indexes
    url_id_idx = CompliantUrlStreamDef.field_idx('id')
    compliant_idx = CompliantUrlStreamDef.field_idx('strategic')
    dest_url_idx = OutlinksStreamDef.field_idx('dst_url_id')
    src_url_idx = OutlinksStreamDef.field_idx('id')
    link_type_idx = OutlinksStreamDef.field_idx('link_type')
    follow_idx = OutlinksStreamDef.field_idx('follow')

    # Find all non compliant url ids
    non_compliant_urlids = set()
    for compliant_entry in stream_compliant:
        compliant = compliant_entry[compliant_idx]
        if compliant is False:
            non_compliant_urlids.add(compliant_entry[url_id_idx])

    # Iterator over outlinks and extract all normal links whose destination
    # is in the *non_compliant_urlids* dict
    for outlink in stream_outlinks:
        dest = outlink[dest_url_idx]
        link_type = outlink[link_type_idx]
        is_follow = 'follow' in outlink[follow_idx]
        if link_type == 'a' and dest in non_compliant_urlids:
            yield (outlink[src_url_idx], 1 if is_follow else 0, dest)


def get_bad_link_counters(stream_bad_links):
    """
    A counter of (url_src_id, error_http_code, count)
    Sorted on `url_src_id`
    """
    # Resolve indexes
    src_url_idx = BadLinksStreamDef.field_idx('id')
    http_code_idx = BadLinksStreamDef.field_idx('http_code')

    # Group by source url_id
    for src_url_id, g in groupby(stream_bad_links, lambda x: x[src_url_idx]):
        links = list(g)
        cnt = Counter()

        for link in links:
            cnt[link[http_code_idx]] += 1

        for http_code in cnt:
            yield (src_url_id, http_code, cnt[http_code])


def get_link_to_non_compliant_urls_counters(stream_non_compliant_links):
    """
    Compute a stream of links to non compliant urls count, given an input
    stream based on LinksToNonCompliantStreamDef.
    The result stream is based on LinksToNonCompliantCountersStreamDef.
    :param stream_non_compliant_links: the input stream (based LinksToNonCompliantStreamDef)
    :type stream_non_compliant_links: iterator
    :returns: iterator
    """
    # Resolve indexes
    src_url_idx = LinksToNonCompliantStreamDef.field_idx('id')

    # Group by source url_id
    for src_url_id, g in groupby(
            stream_non_compliant_links, lambda x: x[src_url_idx]):
        total = 0
        dests = set()
        for _, follow, dest in g:
            if not follow:
                continue
            dests.add(dest)
            total += 1

        yield (src_url_id, len(dests), total)
