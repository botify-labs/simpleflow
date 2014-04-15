from itertools import izip
from cdf.core.streams.exceptions import GroupWithSkipException
from cdf.core.streams.utils import idx_from_stream
from cdf.utils.date import date_2k_mn_to_date
from cdf.metadata.raw import STREAMS_HEADERS


__all__ = ["PROCESSORS", "FINAL_PROCESSORS", "GENERATOR_FILES"]


def _extract_stream_fields(stream_identifier, stream):
    """
    :param stream_identifier: stream's id, like 'ids', 'infos'
    :return: a dict containing `field: value` mapping
    """
    return {field[0]: value for field, value in
            izip(STREAMS_HEADERS[stream_identifier], stream)}


def _process_infos(doc, stream_infos):
    """Process `urlinfos` stream
    """
    date_crawled_idx = idx_from_stream('infos', 'date_crawled')
    stream_infos[date_crawled_idx] = date_2k_mn_to_date(
        stream_infos[date_crawled_idx]).strftime("%Y-%m-%dT%H:%M:%S")
    # TODO could skip non-crawled url here
    # http code 0, 1, 2 are reserved for non-crawled urls

    doc.update(_extract_stream_fields('INFOS', stream_infos))
    # infos_mask has a special process
    del(doc['infos_mask'])

    # `?` should be rename to `not-set`
    if doc['content_type'] == '?':
        doc['content_type'] = 'not-set'

    # rename `delay1` and `delay2`
    # they are kept in the stream schema for compatibility with
    doc['delay_first_byte'] = doc['delay1']
    doc['delay_last_byte'] = doc['delay2']
    del(doc['delay1'])
    del(doc['delay2'])

    # mask:
    # 1 gzipped, 2 notused, 4 meta_noindex
    # 8 meta_nofollow 16 has_canonical 32 bad canonical
    infos_mask = stream_infos[idx_from_stream('infos', 'infos_mask')]
    doc['gzipped'] = 1 & infos_mask == 1

    target = doc['metadata']['robots']
    target['noindex'] = 4 & infos_mask == 4
    target['nofollow'] = 8 & infos_mask == 8


def _process_suggest(document, stream_suggests):
    url_id, pattern_hash = stream_suggests
    document['patterns'].append(pattern_hash)


def _process_final(document):
    """Final process the whole generated document

    It does several things:
        - remove temporary attributes used by other processing
        - remove non-crawled url document unless it receives redirection
          or canonical links
        - some analytic processing that needs a global view of the whole
          document
        - control the size of some list (eg. list of links)
    """
    # include not crawled url in generated document only if they've received
    # redirection or canonicals
    if document['http_code'] in (0, 1, 2):
        redirect_from_nb = document['redirect']['from']['nb']
        canonical_from_nb = document['canonical']['from']['nb']
        if redirect_from_nb > 0 or canonical_from_nb > 0:
            url = document['url']
            url_id = document['id']
            document.clear()
            document.update({
                'id': url_id,
                'url': url,
                'http_code': 0
            })
        else:
            raise GroupWithSkipException()

PROCESSORS = {
    'infos': _process_infos,
    'suggest': _process_suggest,
}

FINAL_PROCESSORS = [_process_final]

GENERATOR_FILES = [
    "urlinfos",
    "url_suggested_clusters"
]
