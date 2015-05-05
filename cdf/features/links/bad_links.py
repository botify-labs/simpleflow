# import logging
from itertools import groupby, ifilter
from cdf.features.links.helpers.predicates import is_follow_link

from cdf.features.main.streams import InfosStreamDef, CompliantUrlStreamDef
from cdf.features.links.streams import (
    OutlinksStreamDef,
    BadLinksStreamDef,
    LinksToNonCompliantStreamDef,
    LinksToNonCanonicalStreamDef
)


# logger = logging.getLogger(__name__)


def get_bad_links(stream_infos, stream_outlinks):
    """Detects bad links

    Result stream is based on BadLinksStreamDef
    """
    # Resolve indexes
    http_code_idx = InfosStreamDef.field_idx('http_code')
    url_id_idx = InfosStreamDef.field_idx('id')
    dest_url_idx = OutlinksStreamDef.field_idx('dst_url_id')
    src_url_idx = OutlinksStreamDef.field_idx('id')
    link_type_idx = OutlinksStreamDef.field_idx('link_type')
    follow_idx = OutlinksStreamDef.field_idx('follow')

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
        is_follow = 'follow' in outlink[follow_idx]
        if link_type == 'a' and dest in bad_code:
            yield (outlink[src_url_idx], dest,
                   1 if is_follow else 0, bad_code[dest])


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
    compliant_idx = CompliantUrlStreamDef.field_idx('compliant')
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
    """Count bad links by url_id and http_code

    Result stream is based on BadLinksCountersStreamDef
    """
    # Resolve indexes
    src_url_idx = BadLinksStreamDef.field_idx('id')

    # Group by source url_id
    for src_url_id, group in groupby(
            stream_bad_links, lambda x: x[src_url_idx]):
        links = sorted([
            (code, dest) for (_, dest, follow, code) in group
            if follow
        ])

        # group by http_code
        for code, code_group in groupby(links, lambda x: x[0]):
            dests = set(dest for _, dest in code_group)
            yield (src_url_id, code, len(dests))


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


def get_links_to_non_canonical(stream_urllinks):
    # First, get bad canonicals

    # logger.info('Reading canonicals')
    bad_canonicals = set()
    read_canons = set()  # ignore 2nd+ canonical
    for src_uid, link_type, follow_mask, dst_uid, dst_url in stream_urllinks:
        if link_type == 'canonical' and src_uid not in read_canons:
            if src_uid != dst_uid:
                bad_canonicals.add(src_uid)
            read_canons.add(src_uid)
    del read_canons
    # logger.info('Found %d non-self canonicals', len(bad_canonicals))

    # Then get links to them
    links_to_non_canonical = []
    for src_uid, link_type, follow_mask, dst_uid, dst_url in stream_urllinks:
        if dst_uid in bad_canonicals and link_type != 'canonical':
            links_to_non_canonical.append(
                (src_uid,
                 int(is_follow_link(follow_mask, True)),
                 dst_uid
                 ))
    # logger.info('Found %d links to non-self canonicals', len(links_to_non_canonical))

    return links_to_non_canonical


def get_links_to_non_canonical_counters(stream_links_to_non_canonical):
    # Cut'n' pasted from get_link_to_non_compliant_urls_counters.

    # Resolve indexes
    src_url_idx = LinksToNonCanonicalStreamDef.field_idx('id')

    # Group by source url_id
    for src_url_id, g in groupby(
            stream_links_to_non_canonical, lambda x: x[src_url_idx]):
        # total = 0
        dests = set()
        for _, follow, dest in g:
            if not follow:
                continue
            dests.add(dest)
            # total += 1

        yield (
            src_url_id, len(dests)  # , total
        )
