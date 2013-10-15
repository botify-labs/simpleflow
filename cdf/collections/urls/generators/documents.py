import ujson
from itertools import izip

from cdf.streams.mapping import STREAMS_HEADERS, CONTENT_TYPE_INDEX
from cdf.log import logger
from cdf.streams.transformations import group_with
from cdf.streams.exceptions import GroupWithSkipException
from cdf.streams.utils import idx_from_stream
from cdf.streams.masks import list_to_mask
from cdf.utils.date import date_2k_mn_to_date
from cdf.utils.hashing import string_to_int64
from cdf.collections.urls.utils import children_from_field
from cdf.collections.urls.constants import SUGGEST_CLUSTERS


def extract_patterns(attributes, stream_item):
    # Create initial dictionary
    attributes.update({i[0]: value for i, value in izip(STREAMS_HEADERS['PATTERNS'], stream_item)})

    attributes['url'] = attributes['protocol'] + '://' + ''.join((attributes['host'], attributes['path'], attributes['query_string']))
    attributes['url_hash'] = string_to_int64(attributes['url'])

    # query_string fields
    query_string = stream_item[4]
    if query_string:
        # The first character is ? we flush it in the split
        qs = [k.split('=') if '=' in k else [k, ''] for k in query_string[1:].split('&')]
        attributes['query_string_keys'] = [q[0] for q in qs]
        attributes['query_string_keys_order'] = ';'.join(attributes['query_string_keys'])
        attributes['query_string_items'] = qs
    attributes['metadata_nb'] = {verbose_content_type: 0 for verbose_content_type in CONTENT_TYPE_INDEX.itervalues()}
    attributes['inlinks_internal_nb'] = {_f.split('.')[1]: 0 for _f in children_from_field('inlinks_internal_nb')}
    attributes['inlinks_internal_nb']['nofollow_combinations'] = []
    attributes['inlinks_internal'] = []
    attributes['outlinks_internal_nb'] = {_f.split('.')[1]: 0 for _f in children_from_field('outlinks_internal_nb')}
    attributes['outlinks_internal_nb']['nofollow_combinations'] = []
    attributes['outlinks_external_nb'] = {_f.split('.')[1]: 0 for _f in children_from_field('outlinks_external_nb')}
    attributes['outlinks_external_nb']['nofollow_combinations'] = []
    attributes['outlinks_internal'] = []
    attributes["inlinks_id_to_idx"] = {}
    attributes["outlinks_id_to_idx"] = {}
    attributes["suggest"] = {c[1]: [] for c in SUGGEST_CLUSTERS}


def extract_infos(attributes, stream_item):
    date_crawled_idx = idx_from_stream('infos', 'date_crawled')

    """
    Those codes should not be returned
    Some pages can be in the queue and not crawled
    from some reason (ex : max pages < to the queue
    ---------------
    job_not_done=0,
    job_todo=1,
    job_in_progress=2,
    """

    stream_item[date_crawled_idx] = date_2k_mn_to_date(stream_item[date_crawled_idx]).strftime("%Y-%m-%dT%H:%M:%S")
    attributes.update({i[0]: value for i, value in izip(STREAMS_HEADERS['INFOS'], stream_item) if i[0] != 'infos_mask'})

    # Reminder : 1 gzipped, 2 notused, 4 meta_noindex 8 meta_nofollow 16 has_canonical 32 bad canonical
    infos_mask = stream_item[idx_from_stream('infos', 'infos_mask')]
    attributes['gzipped'] = 1 & infos_mask == 1
    attributes['meta_noindex'] = 4 & infos_mask == 4
    attributes['meta_nofollow'] = 8 & infos_mask == 8


def extract_contents(attributes, stream_item):
    content_type_id = stream_item[idx_from_stream('contents', 'content_type')]
    txt = stream_item[idx_from_stream('contents', 'txt')]
    if "metadata" not in attributes:
        attributes["metadata"] = {}

    verbose_content_type = CONTENT_TYPE_INDEX[content_type_id]
    if verbose_content_type not in attributes["metadata"]:
        attributes["metadata"][verbose_content_type] = [txt]
    else:
        attributes["metadata"][verbose_content_type].append(txt)

    attributes["metadata_nb"][verbose_content_type] += 1


def extract_outlinks(attributes, stream_item):
    url_src, link_type, follow_keys, url_dst, external_url = stream_item

    def increments_follow_unique():
        if not (url_dst, mask) in attributes["outlinks_id_to_idx"]:
            attributes[outlink_type]['follow_unique'] += 1

    def increments_nofollow_combination():
        found = False
        for _d in attributes[outlink_type]['nofollow_combinations']:
            if _d["key"] == follow_keys:
                _d["value"] += 1
                found = True
                break
        if not found:
            attributes[outlink_type]['nofollow_combinations'].append(
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

        attributes[outlink_type]['total'] += 1
        attributes[outlink_type]['follow' if is_follow else 'nofollow'] += 1
        if is_internal and is_follow:
            increments_follow_unique()
        elif not is_follow:
            increments_nofollow_combination()

        if is_internal:
            url_idx = attributes["outlinks_id_to_idx"].get((url_dst, mask), None)
            if url_idx is not None:
                attributes["outlinks_internal"][url_idx][2] += 1
            else:
                attributes["outlinks_internal"].append([url_dst, mask, 1])
                attributes["outlinks_id_to_idx"][(url_dst, mask)] = len(attributes["outlinks_internal"]) - 1

    elif link_type.startswith('r'):
        http_code = link_type[1:]
        if url_dst == -1:
            attributes['redirects_to'] = {'url': external_url, 'http_code': int(http_code)}
        else:
            attributes['redirects_to'] = {'url_id': url_dst, 'http_code': int(http_code)}
    elif link_type == "canonical":
        attributes['canonical_equals'] = url_src == url_dst
        if url_dst > 0:
            attributes['canonical_to'] = {'id': url_dst}
        else:
            attributes['canonical_to'] = {'url': external_url}


def extract_inlinks(attributes, stream_item):
    url_dst, link_type, follow_keys, url_src = stream_item

    def increments_follow_unique():
        if not (url_src, mask) in attributes["inlinks_id_to_idx"]:
            attributes['inlinks_internal_nb']['follow_unique'] += 1

    def increments_nofollow_combination():
        found = False
        for _d in attributes['inlinks_internal_nb']['nofollow_combinations']:
            if _d["key"] == follow_keys:
                _d["value"] += 1
                found = True
                break
        if not found:
            attributes['inlinks_internal_nb']['nofollow_combinations'].append(
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

        attributes['inlinks_internal_nb']['total'] += 1
        attributes['inlinks_internal_nb']['follow' if is_follow else 'nofollow'] += 1
        if is_follow:
            increments_follow_unique()
        else:
            increments_nofollow_combination()

        url_idx = attributes["inlinks_id_to_idx"].get((url_src, mask), None)
        if url_idx is not None:
            attributes["inlinks_internal"][url_idx][2] += 1
        else:
            attributes["inlinks_internal"].append([url_src, mask, 1])
            attributes["inlinks_id_to_idx"][(url_src, mask)] = len(attributes["inlinks_internal"]) - 1

    elif link_type.startswith('r'):
        http_code = int(link_type[1:])
        if 'redirects_from' not in attributes:
            attributes['redirects_from'] = []
            attributes['redirects_from_nb'] = 0

        attributes['redirects_from_nb'] += 1
        if len(attributes['redirects_from']) < 300:
            attributes['redirects_from'].append({'url_id': url_src, 'http_code': http_code})

    elif link_type == "canonical":
        nb_duplicates = attributes.get('canonical_from_nb', 0) + 1
        attributes['canonical_from_nb'] = nb_duplicates
        if nb_duplicates == 1:
            attributes['canonical_from'] = [url_src]
        else:
            attributes['canonical_from'].append(url_src)


def extract_suggest(attributes, stream_item):
    url_id, section, stype, query, query_hash = stream_item
    attributes['suggest'][stype].append(query_hash)


def end_extract_url(attributes):
    """
    If the url has not been crawled but received redirections or canonicals, we exceptionnaly
    this one into elasticsearch
    """
    del attributes["outlinks_id_to_idx"]
    del attributes["inlinks_id_to_idx"]

    for link_direction in ('inlinks_internal', 'outlinks_internal'):
        if len(attributes[link_direction]) > 300:
            attributes[link_direction] = attributes[link_direction][0:300]

    if attributes['http_code'] in (0, 1, 2):
        if 'redirects_from_nb' in attributes or 'canonical_from_nb' in attributes:
            url = attributes['url']
            url_id = attributes['id']
            attributes.clear()
            attributes.update({
                'id': url_id,
                'url': url,
                'http_code': 0
            })
        else:
            raise GroupWithSkipException()


class UrlDocumentGenerator(object):
    EXTRACTORS = {
        'infos': extract_infos,
        'contents': extract_contents,
        'inlinks': extract_inlinks,
        'outlinks': extract_outlinks,
        'suggest': extract_suggest
    }

    def __init__(self, stream_patterns, **kwargs):
        self.stream_patterns = stream_patterns
        self.streams = kwargs

    """
    Return a document collection

    Format : 

        {
            "url": "http://www.site.com/fr/my-article-1",
            "protocol": "http",
            "host": "www.site.com",
            "path": "/fr/my-article-1", 
            "query_string": "?p=comments&offset=10",
            "query_string_keys": ["p", "offset"],
            "query_string_keys_order": "p;offset",
            "query_string_items": [ ["p", "comments"], ["offset", "10] ],
            "id": 1,
            "date": "2013-10-10 09:10:12",
            "depth": 1,
            "data_mask": 3, // See Data Mask explanations
            "http_code": 200,
            "delay1": 120,
            "delay2": 300,
            "outlinks_internal_follow_nb": 4,
            "outlinks_internal_nofollow_nb": 1,
            "outlinks_external_follow_nb": 5, 
            "outlinks_external_nofollow_nb": 2, 
            "bytesize": 14554,
            "inlinks_internal_nb": 100,
            "inlinks_external_nb": 100,
            "metadata": {
                "title": ["My title"],
                "description": ["My description"],
                "h1": ["My first H1", "My second H1"]
            },
        }

    """
    def __iter__(self):
        left = (self.stream_patterns, 0, extract_patterns)
        streams_ref = {key: (self.streams[key], idx_from_stream(key, 'id'), self.EXTRACTORS[key]) for key in self.streams.keys()}
        return group_with(left, final_func=end_extract_url, **streams_ref)

    def save_to_file(self, location):
        for file_type in self.files.iterkeys():
            self.files[file_type].seek(0)
        f = open(location, 'w')
        f.writelines('\n'.join((ujson.encode(l) for l in self)))
        f.close()
