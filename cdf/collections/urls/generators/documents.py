import ujson
from itertools import izip

from cdf.streams.mapping import STREAMS_HEADERS, CONTENT_TYPE_INDEX
from cdf.log import logger
from cdf.streams.transformations import group_with
from cdf.streams.exceptions import GroupWithSkipException
from cdf.streams.utils import idx_from_stream
from cdf.utils.date import date_2k_mn_to_date
from cdf.utils.hashing import string_to_int64


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


def extract_infos(attributes, stream_item):
    date_crawled_idx = idx_from_stream('infos', 'date_crawled')
    http_code_idx = idx_from_stream('infos', 'http_code')

    """
    Those codes should not be returned
    Some pages can be in the queue and not crawled
    from some reason (ex : max pages < to the queue
    ---------------
    job_not_done=0,
    job_todo=1,
    job_in_progress=2,
    """
    if stream_item[http_code_idx] in (0, 1, 2):
        raise GroupWithSkipException()

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
    url_src, link_type, follow_key, url_dst, external_url = stream_item
    if link_type == "a":
        key_nb = "outlinks_{}_nb".format(follow_key)

        if key_nb not in attributes:
            attributes[key_nb] = 1
        else:
            attributes[key_nb] += 1

        if url_dst > 0:
            key_ids = "outlinks_%s_urls" % follow_key
            if key_ids not in attributes:
                attributes[key_ids] = [url_dst]
            # Store only the first 1000 outlinks
            elif url_dst not in attributes[key_ids] and len(attributes[key_ids]) < 1000:
                attributes[key_ids].append(url_dst)
    elif link_type.startswith('r'):
        http_code = link_type[1:]
        attributes['redirect_to'] = {'url': url_dst, 'http_code': int(http_code)}
    elif link_type == "canonical":
        attributes['canonical_equals'] = url_src == url_dst
        attributes['canonical_url'] = url_dst


def extract_inlinks(attributes, stream_item):
    url_dst, link_type, follow_key, url_src = stream_item
    if link_type == "a":
        key_nb = "inlinks_%s_nb" % follow_key

        if key_nb not in attributes:
            attributes[key_nb] = 1
        else:
            attributes[key_nb] += 1

        if url_src > 0:
            key_ids = "inlinks_%s_urls" % follow_key
            if key_ids not in attributes:
                attributes[key_ids] = [url_src]
            elif len(attributes[key_ids]) < 300 and url_src not in attributes[key_ids]:
                attributes[key_ids].append(url_src)

    elif link_type.startswith('r'):
        http_code = int(link_type[1:])
        if 'redirect_from' not in attributes:
            attributes['redirect_from'] = []
            attributes['redirects_nb'] = 0

        attributes['redirects_nb'] += 1
        if len(attributes['redirect_from']) < 300:
            attributes['redirect_from'].append({'url': url_src, 'http_code': http_code})

    elif link_type == "canonical":
        nb_duplicates = attributes.get('canonical_nb_duplicates', 0) + 1
        attributes['canonical_nb_duplicates'] = nb_duplicates
        if nb_duplicates == 1:
            attributes['canonical_duplicate_urls'] = [url_src]
        else:
            attributes['canonical_duplicate_urls'].append(url_src)


class UrlDocumentGenerator(object):
    EXTRACTORS = {
        'infos': extract_infos,
        'contents': extract_contents,
        'inlinks': extract_inlinks,
        'outlinks': extract_outlinks,
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
            "url_id": 1,
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
            "inlinks_nb": 100,
            "inlinks_follow_nb": 90,
            "metadata": {
                "title": ["My title"],
                "description": ["My description"],
                "h1": ["My first H1", "My second H1"]
            },
            "outlinks_follow_ids": [10, 20, 50 ...],
            "outlinks_nofollow_ids": [32],
            "inlinks_follow_ids": [10, 20, 23...] // The 1000 first found in the crawl
            "inlinks_nofollow_ids": [30, ...] // The 1000 first found in the crawl
        }

    """
    def __iter__(self):
        left = (self.stream_patterns, 0, extract_patterns)
        streams_ref = {key: (self.streams[key], idx_from_stream(key, 'id'), self.EXTRACTORS[key]) for key in self.streams.keys()}
        return group_with(left, **streams_ref)

    def save_to_file(self, location):
        for file_type in self.files.iterkeys():
            self.files[file_type].seek(0)
        f = open(location, 'w')
        f.writelines('\n'.join((ujson.encode(l) for l in self)))
        f.close()
