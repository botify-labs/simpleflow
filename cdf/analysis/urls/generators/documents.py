import ujson
from itertools import izip
from cdf.analysis.urls.utils import is_link_internal
from cdf.metadata.raw import (STREAMS_HEADERS, CONTENT_TYPE_INDEX,
                              MANDATORY_CONTENT_TYPES)
from cdf.core.streams.transformations import group_with
from cdf.core.streams.exceptions import GroupWithSkipException
from cdf.core.streams.utils import idx_from_stream
from cdf.metadata.raw.masks import list_to_mask
from cdf.utils.date import date_2k_mn_to_date
from cdf.utils.hashing import string_to_int64
from cdf.metadata.url import ELASTICSEARCH_BACKEND


# TODO refactor into an ORM fashion
#   data format => hierarchy of classes (ORM)
#   ORM objects knows how to index themselves to ES
# maybe ElasticUtils will be a good reference


def _init_document(doc):
    return ELASTICSEARCH_BACKEND.default_document(
        document=doc)


def _clean_document(doc):
    def _recursive_clean(doc, access):
        for k, v in doc.items():
            if isinstance(v, dict):
                _recursive_clean(v, doc[k])
            elif v is None or v == []:
                del(access[k])
    _recursive_clean(doc, doc)


def _content_types(mandatory=False):
    for content_name in CONTENT_TYPE_INDEX.itervalues():
        if not mandatory:
            yield content_name
        if mandatory and content_name in MANDATORY_CONTENT_TYPES:
            yield content_name


def _get_nofollow_combination_key(keys):
    return '_'.join(sorted(keys))


def _extract_stream_fields(stream_identifier, stream):
    """
    :param stream_identifier: stream's id, like 'ids', 'infos'
    :return: a dict containing `field: value` mapping
    """
    return {field[0]: value for field, value in
            izip(STREAMS_HEADERS[stream_identifier], stream)}


def _process_ids(doc, stream_ids):
    """Init the document and process `urlids` stream
    """
    # init the document with default field values
    # flatten, eg. {'a.b.c': None}
    _init_document(doc)

    # simple information about each url
    doc.update(_extract_stream_fields('PATTERNS', stream_ids))
    doc['url'] = doc['protocol'] + '://' + ''.join(
        (doc['host'], doc['path'], doc['query_string']))
    doc['url_hash'] = string_to_int64(doc['url'])

    query_string = stream_ids[4]
    if query_string:
        # The first character is ? we flush it in the split
        qs = [k.split('=') if '=' in k else [k, '']
              for k in query_string[1:].split('&')]
        doc['query_string_keys'] = [q[0] for q in qs]

    # temporary structures for analytic processing
    # resolve a (src, mask) to its index in `outlinks_internal` list
    doc["inlinks_id_to_idx"] = {}
    # resolve a (dest, mask) to its index in `inlinks_internal` list
    doc["outlinks_id_to_idx"] = {}
    # a temp set to track all `seen` src url of incoming links
    doc["processed_inlink_url"] = set()
    # a temp set to track all `seen` dest url of outgoing links
    doc["processed_outlink_url"] = set()


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


def _process_contents(doc, stream_contents):
    content_type_id = stream_contents[idx_from_stream('contents', 'content_type')]
    content_type = CONTENT_TYPE_INDEX[content_type_id]
    content = stream_contents[idx_from_stream('contents', 'txt')]
    doc['metadata'][content_type]['contents'].append(content)


def _process_metadata_duplicate(doc, stream_duplicate):
    _, metadata_idx, nb_filled, nb_duplicates, is_first, duplicate_urls = stream_duplicate
    metadata_type = CONTENT_TYPE_INDEX[metadata_idx]

    meta = doc['metadata'][metadata_type]
    # number of metadata of this kind
    meta['nb'] = nb_filled
    # number of duplications of this piece of metadata
    dup = meta['duplicates']
    dup['nb'] = nb_duplicates
    # urls that have duplicates
    dup['urls'] = duplicate_urls
    # is this the first one out of all duplicates
    dup['is_first'] = is_first


def _process_outlinks(document, stream_oulinks):
    url_src, link_type, follow_keys, url_dst, external_url = stream_oulinks

    if link_type == "a":
        # is_internal = url_dst > 0
        is_internal = is_link_internal(follow_keys, url_dst)
        is_follow = len(follow_keys) == 1 and follow_keys[0] == "follow"
        outlink_type = "outlinks_internal" if is_internal else "outlinks_external"
        mask = list_to_mask(follow_keys)

        outlink_nb = document[outlink_type]['nb']
        outlink_nb['total'] += 1

        # target dict changes with link follow status
        follow = outlink_nb['follow' if is_follow else 'nofollow']
        follow['total'] += 1
        if is_internal and is_follow:
            # increment follow counters
            if not (url_dst, mask) in document["outlinks_id_to_idx"]:
                follow['unique'] += 1
        elif not is_follow:
            # increment nofollow combination counters
            key = _get_nofollow_combination_key(follow_keys)
            follow['combinations'][key] += 1

        # internal outlinks
        # still need dest url id check since we can have internal url
        # blocked by robots.txt
        if is_internal and url_dst > 0:
            # add this link's dest to the processed set
            document['processed_outlink_url'].add(url_dst)

            url_idx = document["outlinks_id_to_idx"].get((url_dst, mask), None)
            outlink_urls = document['outlinks_internal']['urls']['all']
            if url_idx is not None:
                outlink_urls[url_idx][2] += 1
            else:
                outlink_urls.append([url_dst, mask, 1])
                document["outlinks_id_to_idx"][(url_dst, mask)] = len(outlink_urls) - 1

    elif link_type.startswith('r'):
        http_code = link_type[1:]
        redirects_to = document['redirect']['to']
        if url_dst == -1:
            redirects_to['url'] = external_url
        else:
            redirects_to['url_id'] = url_dst
        redirects_to['http_code'] = int(http_code)

    elif link_type == "canonical":
        canonical_to = document['canonical']['to']
        if canonical_to.get('equal', None) is None:
            # We take only the first canonical found
            canonical_to['equal'] = url_src == url_dst
            if url_dst > 0:
                canonical_to['url_id'] = url_dst
            else:
                canonical_to['url'] = external_url


def _process_inlinks(document, stream_inlinks):
    url_dst, link_type, follow_keys, url_src = stream_inlinks

    if link_type == "a":
        is_follow = len(follow_keys) == 1 and follow_keys[0] == "follow"
        mask = list_to_mask(follow_keys)

        inlink_nb = document['inlinks_internal']['nb']
        inlink_nb['total'] += 1

        follow = inlink_nb['follow' if is_follow else 'nofollow']
        follow['total'] += 1

        # add src to processed set
        document['processed_inlink_url'].add(url_src)

        if is_follow:
            if not (url_src, mask) in document["inlinks_id_to_idx"]:
                follow['unique'] += 1
        else:
            key = _get_nofollow_combination_key(follow_keys)
            follow['combinations'][key] += 1

        url_idx = document["inlinks_id_to_idx"].get((url_src, mask), None)
        inlink_urls = document['inlinks_internal']['urls']
        if url_idx is not None:
            inlink_urls[url_idx][2] += 1
        else:
            inlink_urls.append([url_src, mask, 1])
            document["inlinks_id_to_idx"][(url_src, mask)] = len(inlink_urls) - 1

    elif link_type.startswith('r'):
        # TODO dangerous assumption of crawl's string format to be 'r3xx'
        http_code = int(link_type[1:])
        redirects_from = document['redirect']['from']
        redirects_from['nb'] += 1
        if len(redirects_from['urls']) < 300:
            redirects_from['urls'].append([url_src, http_code])

    elif link_type == "canonical":
        canonical_from = document['canonical']['from']

        # only count for none self canonical
        if url_dst != url_src:
            canonical_from['nb'] += 1
            canonical_from['urls'].append(url_src)


def _process_suggest(document, stream_suggests):
    url_id, pattern_hash = stream_suggests
    document['patterns'].append(pattern_hash)


def _process_badlinks(document, stream_badlinks):
    _, url_dest_id, http_code = stream_badlinks

    errors_nb = document['outlinks_internal']['nb']['errors']
    errors_urls = document['outlinks_internal']['urls']

    error_kind = None
    if 300 <= http_code < 400:
        error_kind = '3xx'
    elif 400 <= http_code < 500:
        error_kind = '4xx'
    elif http_code >= 500:
        error_kind = '5xx'

    errors_nb[error_kind] += 1
    if len(errors_urls[error_kind]) < 10:
        errors_urls[error_kind].append(url_dest_id)

    # increment the consolidate value
    errors_nb['total'] += 1


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
    document['outlinks_internal']['nb']['unique'] = len(document['processed_outlink_url'])
    document['inlinks_internal']['nb']['unique'] = len(document['processed_inlink_url'])

    # delete intermediate data structures
    del document['processed_inlink_url']
    del document['processed_outlink_url']
    del document["outlinks_id_to_idx"]
    del document["inlinks_id_to_idx"]

    # TODO can be restricted in link process
    # only push up to 300 links information for each url
    for link_direction in ('inlinks_internal', 'outlinks_internal'):
        if len(document[link_direction]) > 300:
            document[link_direction] = document[link_direction][0:300]

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

    _clean_document(document)


class UrlDocumentGenerator(object):
    """Aggregates incoming streams, produces a json document for each url

    Format see `cdf.metadata.url` package
    """
    PROCESSORS = {
        'infos': _process_infos,
        'contents': _process_contents,
        'contents_duplicate': _process_metadata_duplicate,
        'inlinks': _process_inlinks,
        'outlinks': _process_outlinks,
        'suggest': _process_suggest,
        'badlinks': _process_badlinks
    }

    def __init__(self, stream_patterns, **kwargs):
        self.stream_patterns = stream_patterns
        self.streams = kwargs

        # `urlids` is the reference stream
        left = (self.stream_patterns, 0, _process_ids)
        streams_ref = {key: (self.streams[key], idx_from_stream(key, 'id'),
                             self.PROCESSORS[key])
                       for key in self.streams.keys()}
        self.generator = group_with(left, final_func=_process_final,
                                    **streams_ref)

    def __iter__(self):
        return self.generator

    def next(self):
        return next(self.generator)

    def save_to_file(self, location):
        for file_type in self.files.iterkeys():
            self.files[file_type].seek(0)
        f = open(location, 'w')
        f.writelines('\n'.join((ujson.encode(l) for l in self)))
        f.close()
