import ujson
from itertools import izip
from collections import defaultdict

from cdf.metadata.raw import (STREAMS_HEADERS, CONTENT_TYPE_INDEX,
                              MANDATORY_CONTENT_TYPES)
from cdf.core.streams.transformations import group_with
from cdf.core.streams.exceptions import GroupWithSkipException
from cdf.core.streams.utils import idx_from_stream
from cdf.metadata.raw.masks import list_to_mask
from cdf.utils.date import date_2k_mn_to_date
from cdf.utils.hashing import string_to_int64
from cdf.metadata.url import get_children
from cdf.metadata.url import URLS_DATA_FORMAT_DEFINITION
from cdf.metadata.url.es_backend_utils import generate_empty_document

# TODO refactor into an ORM fashion
#   data format => hierarchy of classes (ORM)
#   ORM objects knows how to index themselves to ES
# maybe ElasticUtils will be a good reference


def _content_types(mandatory=False):
    for content_name in CONTENT_TYPE_INDEX.itervalues():
        if not mandatory:
            yield content_name
        if mandatory and content_name in MANDATORY_CONTENT_TYPES:
            yield content_name


def _extract_stream_fields(stream_identifier, stream):
    """
    :param stream_identifier: stream's id, like 'ids', 'infos'
    :return: a dict containing `field: value` mapping
    """
    return {field[0]: value for field, value in
            izip(STREAMS_HEADERS[stream_identifier], stream)}


def _process_ids(document, stream_ids):
    """Init the document and process `urlids` stream
    """
    # TODO `patterns` here should be renamed to ids
    document.update(_extract_stream_fields('PATTERNS', stream_ids))
    document['url'] = document['protocol'] + '://' + ''.join(
        (document['host'], document['path'], document['query_string']))
    document['url_hash'] = string_to_int64(document['url'])

    # query_string fields
    query_string = stream_ids[4]
    if query_string:
        # The first character is ? we flush it in the split
        qs = [k.split('=') if '=' in k else [k, ''] for k in query_string[1:].split('&')]
        document['query_string_keys'] = [q[0] for q in qs]
    document['metadata_nb'] = {content_name: 0 for content_name
                               in _content_types()}
    document['metadata_duplicate'] = {content_name: [] for content_name
                                      in _content_types(mandatory=True)}
    document['metadata_duplicate_nb'] = {content_name: [] for content_name
                                         in _content_types(mandatory=True)}
    document['metadata_duplicate_is_first'] = {content_name: [] for content_name
                                               in _content_types(mandatory=True)}
    document['inlinks_internal_nb'] = {field.split('.')[1]: 0 for field
                                       in get_children('inlinks_internal_nb')}
    document['inlinks_internal_nb']['nofollow_combinations'] = []
    # a list of [src, mask, count]
    document['inlinks_internal'] = []
    document['outlinks_internal_nb'] = {field.split('.')[1]: 0 for field
                                        in get_children('outlinks_internal_nb')}
    document['outlinks_internal_nb']['nofollow_combinations'] = []
    document['outlinks_external_nb'] = {field.split('.')[1]: 0 for field
                                        in get_children('outlinks_external_nb')}
    document['outlinks_external_nb']['nofollow_combinations'] = []
    # a list of [dest, mask, count]
    document['outlinks_internal'] = []
    # resolve a (src, mask) to its index in `outlinks_internal` list
    document["inlinks_id_to_idx"] = {}
    # resolve a (dest, mask) to its index in `inlinks_internal` list
    document["outlinks_id_to_idx"] = {}
    # a temp set to track all `seen` src url of incoming links
    document["processed_inlink_url"] = set()
    # a temp set to track all `seen` dest url of outgoing links
    document["processed_outlink_url"] = set()


def _process_infos(document, stream_infos):
    """Process `urlinfos` stream
    """
    date_crawled_idx = idx_from_stream('infos', 'date_crawled')
    stream_infos[date_crawled_idx] = date_2k_mn_to_date(
        stream_infos[date_crawled_idx]).strftime("%Y-%m-%dT%H:%M:%S")
    # TODO could skip non-crawled url here
    # http code 0, 1, 2 are reserved for non-crawled urls

    document.update(_extract_stream_fields('INFOS', stream_infos))
    # infos_mask has a special process
    del(document['infos_mask'])

    # `?` should be rename to `not-set`
    if document['content_type'] == '?':
        document['content_type'] = 'not-set'

    # mask:
    # 1 gzipped, 2 notused, 4 meta_noindex
    # 8 meta_nofollow 16 has_canonical 32 bad canonical
    infos_mask = stream_infos[idx_from_stream('infos', 'infos_mask')]
    document['gzipped'] = 1 & infos_mask == 1
    document['meta_noindex'] = 4 & infos_mask == 4
    document['meta_nofollow'] = 8 & infos_mask == 8


def _process_contents(attributes, stream_item):
    content_type_id = stream_item[idx_from_stream('contents', 'content_type')]
    txt = stream_item[idx_from_stream('contents', 'txt')]
    if "metadata" not in attributes:
        attributes["metadata"] = {}

    verbose_content_type = CONTENT_TYPE_INDEX[content_type_id]
    if verbose_content_type not in attributes["metadata"]:
        attributes["metadata"][verbose_content_type] = [txt]
    else:
        attributes["metadata"][verbose_content_type].append(txt)


def _process_metadata_duplicate(document, stream_duplicate):
    _, metadata_idx, nb_filled, nb_duplicates, is_first, duplicate_urls = stream_duplicate
    metadata_type = CONTENT_TYPE_INDEX[metadata_idx]
    # number of metadata of this kind
    document['metadata_nb'][metadata_type] = nb_filled
    # number of duplications of this piece of metadata
    document['metadata_duplicate_nb'][metadata_type] = nb_duplicates
    # urls that have duplicates
    document['metadata_duplicate'][metadata_type] = duplicate_urls
    # is this the first one out of all duplicates
    document['metadata_duplicate_is_first'][metadata_type] = is_first


def _process_outlinks(document, stream_oulinks):
    url_src, link_type, follow_keys, url_dst, external_url = stream_oulinks

    def increments_follow_unique():
        if not (url_dst, mask) in document["outlinks_id_to_idx"]:
            document[outlink_type]['follow_unique'] += 1

    def increments_nofollow_combination():
        found = False
        for _d in document[outlink_type]['nofollow_combinations']:
            if _d["key"] == follow_keys:
                _d["value"] += 1
                found = True
                break
        if not found:
            document[outlink_type]['nofollow_combinations'].append(
                {
                    "key": follow_keys,
                    "value": 1
                }
            )

    def follow_keys_to_acronym():
        return "".join(k[0] for k in sorted(follow_keys))

    if link_type == "a":
        is_internal = url_dst > 0
        is_follow = len(follow_keys) == 1 and follow_keys[0] == "follow"
        outlink_type = "outlinks_internal_nb" if is_internal else "outlinks_external_nb"
        mask = list_to_mask(follow_keys)

        document[outlink_type]['total'] += 1
        document[outlink_type]['follow' if is_follow else 'nofollow'] += 1
        if is_internal and is_follow:
            increments_follow_unique()
        elif not is_follow:
            increments_nofollow_combination()

        if is_internal:
            # add this link's dest to the processed set
            document['processed_outlink_url'].add(url_dst)

            url_idx = document["outlinks_id_to_idx"].get((url_dst, mask), None)
            if url_idx is not None:
                document["outlinks_internal"][url_idx][2] += 1
            else:
                document["outlinks_internal"].append([url_dst, mask, 1])
                document["outlinks_id_to_idx"][(url_dst, mask)] = len(document["outlinks_internal"]) - 1
    elif link_type.startswith('r'):
        http_code = link_type[1:]
        if url_dst == -1:
            document['redirects_to'] = {'url': external_url, 'http_code': int(http_code)}
        else:
            document['redirects_to'] = {'url_id': url_dst, 'http_code': int(http_code)}
    elif link_type == "canonical":
        if not 'canonical_to_equal' in document:
            # We take only the first canonical found
            document['canonical_to_equal'] = url_src == url_dst
            if url_dst > 0:
                document['canonical_to'] = {'url_id': url_dst}
            else:
                document['canonical_to'] = {'url': external_url}


def _process_inlinks(document, stream_inlinks):
    url_dst, link_type, follow_keys, url_src = stream_inlinks

    def increments_follow_unique():
        if not (url_src, mask) in document["inlinks_id_to_idx"]:
            document['inlinks_internal_nb']['follow_unique'] += 1

    def increments_nofollow_combination():
        found = False
        for _d in document['inlinks_internal_nb']['nofollow_combinations']:
            if _d["key"] == follow_keys:
                _d["value"] += 1
                found = True
                break
        if not found:
            document['inlinks_internal_nb']['nofollow_combinations'].append(
                {
                    "key": follow_keys,
                    "value": 1
                }
            )

    def follow_keys_to_acronym():
        return "".join(k[0] for k in sorted(follow_keys))

    if link_type == "a":
        is_follow = len(follow_keys) == 1 and follow_keys[0] == "follow"
        mask = list_to_mask(follow_keys)

        document['inlinks_internal_nb']['total'] += 1
        document['inlinks_internal_nb']['follow' if is_follow else 'nofollow'] += 1

        # add src to processed set
        document['processed_inlink_url'].add(url_src)

        if is_follow:
            increments_follow_unique()
        else:
            increments_nofollow_combination()

        url_idx = document["inlinks_id_to_idx"].get((url_src, mask), None)
        if url_idx is not None:
            document["inlinks_internal"][url_idx][2] += 1
        else:
            document["inlinks_internal"].append([url_src, mask, 1])
            document["inlinks_id_to_idx"][(url_src, mask)] = len(document["inlinks_internal"]) - 1

    elif link_type.startswith('r'):
        # TODO dangerous assumption of crawl's string format to be 'r3xx'
        http_code = int(link_type[1:])
        if 'redirects_from' not in document:
            document['redirects_from'] = []
            document['redirects_from_nb'] = 0

        document['redirects_from_nb'] += 1
        if len(document['redirects_from']) < 300:
            document['redirects_from'].append({'url_id': url_src, 'http_code': http_code})

    elif link_type == "canonical":
        current_nb = document.get('canonical_from_nb', 0)

        if current_nb is 0:
            # init the counter
            document['canonical_from_nb'] = 0

        # only count for none self canonical
        if url_dst != url_src:
            current_nb += 1
            document['canonical_from_nb'] = current_nb

            if current_nb == 1:
                document['canonical_from'] = [url_src]
            else:
                document['canonical_from'].append(url_src)


def _process_suggest(document, stream_suggests):
    url_id, pattern_hash = stream_suggests
    if 'patterns' not in document:
        document['patterns'] = [pattern_hash]
    else:
        document['patterns'].append(pattern_hash)


def _process_badlinks(document, stream_badlinks):
    _, url_dest_id, http_code = stream_badlinks
    error_link_key = 'error_links'
    if error_link_key not in document:
        document[error_link_key] = defaultdict(lambda: {'nb': 0, 'urls': []})

    target_dict = document[error_link_key]

    if 300 <= http_code < 400:
        target_dict['3xx']['nb'] += 1
        if len(target_dict['3xx']['urls']) < 10:
            target_dict['3xx']['urls'].append(url_dest_id)
    elif 400 <= http_code < 500:
        target_dict['4xx']['nb'] += 1
        if len(target_dict['4xx']['urls']) < 10:
            target_dict['4xx']['urls'].append(url_dest_id)
    elif http_code >= 500:
        target_dict['5xx']['nb'] += 1
        if len(target_dict['5xx']['urls']) < 10:
            target_dict['5xx']['urls'].append(url_dest_id)

    # increment `any.nb` as a consolidate value
    target_dict['any']['nb'] += 1
    if 'urls' in target_dict['any']:
        del target_dict['any']['urls']


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
    document['outlinks_internal_nb']['total_unique'] = len(document['processed_outlink_url'])
    document['inlinks_internal_nb']['total_unique'] = len(document['processed_inlink_url'])

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
        if 'redirects_from_nb' in document or 'canonical_from_nb' in document:
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

    def __iter__(self):
        # `urlids` is the reference stream
        left = (self.stream_patterns, 0, _process_ids)
        streams_ref = {key: (self.streams[key], idx_from_stream(key, 'id'),
                             self.PROCESSORS[key])
                       for key in self.streams.keys()}
        return group_with(left, final_func=_process_final, **streams_ref)

    def save_to_file(self, location):
        for file_type in self.files.iterkeys():
            self.files[file_type].seek(0)
        f = open(location, 'w')
        f.writelines('\n'.join((ujson.encode(l) for l in self)))
        f.close()