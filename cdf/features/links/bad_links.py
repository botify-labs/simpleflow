from collections import Counter
from itertools import groupby

from cdf.features.main.streams import InfosStreamDef, StrategicUrlStreamDef
from cdf.features.links.streams import (
    OutlinksStreamDef,
    BadLinksStreamDef,
    LinksToNonStrategicStreamDef
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
        if (http_code >= 300):
            bad_code[info[url_id_idx]] = http_code

    # Iterator over outlinks and extract all normal links whose destination
    # is in the *bad_code* dict
    for outlink in stream_outlinks:
        dest = outlink[dest_url_idx]
        link_type = outlink[link_type_idx]
        if link_type == 'a' and dest in bad_code:
            yield (outlink[src_url_idx], dest, bad_code[dest])


def get_links_to_non_strategic_urls(stream_strategic, stream_outlinks):
    """Compute a stream of outlinks to non strategic urls.
    The result stream is based on LinksToNonStrategicStreamDef.
    :param stream_strategic: a stream of strategic urls (based on StrategicUrlStreamDef)
    :type stream_strategic: iterable
    :param stream_outlinks: a stream of outlinks (based on OutlinksStreamDef)
    :type stream_outlinks: iterable
    :returns: iterable - the stream of outlinks to non strategic urls.
    """
    # Resolve indexes
    url_id_idx = StrategicUrlStreamDef.field_idx('id')
    strategic_idx = StrategicUrlStreamDef.field_idx('strategic')
    dest_url_idx = OutlinksStreamDef.field_idx('dst_url_id')
    src_url_idx = OutlinksStreamDef.field_idx('id')
    link_type_idx = OutlinksStreamDef.field_idx('link_type')

    # Find all non strategic url ids
    non_strategic_urlids = set()
    for strategic_entry in stream_strategic:
        strategic = strategic_entry[strategic_idx]
        if strategic is False:
            non_strategic_urlids.add(strategic_entry[url_id_idx])

    # Iterator over outlinks and extract all normal links whose destination
    # is in the *non_strategic_urlids* dict
    for outlink in stream_outlinks:
        dest = outlink[dest_url_idx]
        link_type = outlink[link_type_idx]
        if link_type == 'a' and dest in non_strategic_urlids:
            yield (outlink[src_url_idx], dest)


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


def get_link_to_non_strategic_urls_counters(stream_non_strategic_links):
    """
    Compute a stream of links to non strategic urls count, given an input
    stream based on LinksToNonStrategicStreamDef.
    The result stream is based on LinksToNonStrategicCountersStreamDef.
    :param stream_non_strategic_links: the input stream (based LinksToNonStrategicStreamDef)
    :type stream_non_strategic_links: iterator
    :returns: iterator
    """
    # Resolve indexes
    src_url_idx = LinksToNonStrategicStreamDef.field_idx('id')

    # Group by source url_id
    for src_url_id, g in groupby(stream_non_strategic_links, lambda x: x[src_url_idx]):
        yield (src_url_id, len(list(g)))
