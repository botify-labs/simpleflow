import ujson
from itertools import izip


from cdf.settings import STREAMS_HEADERS, CONTENT_TYPE_INDEX
from cdf.log import logger
from cdf.streams.transformations import group_with
from cdf.streams.utils import idx_from_stream
from cdf.utils.date import date_2k_mn_to_date

def extract_patterns(attributes, stream_item):
    # Create initial dictionary
    attributes.update({i[0]: value for i, value in izip(STREAMS_HEADERS['PATTERNS'], stream_item)})

    # query_string fields
    query_string = stream_item[4]
    if query_string:
        # The first character is ? we flush it in the split
        qs = [k.split('=') if '=' in k else [k, ''] for k in query_string[1:].split('&')]
        attributes['query_string_keys'] = [q[0] for q in qs]
        attributes['query_string_keys_order'] = ';'.join(attributes['query_string_keys'])
        attributes['query_string_items'] = qs


def extract_infos(attributes, stream_item):
    date_crawled_idx = idx_from_stream('infos', 'date_crawled')
    stream_item[date_crawled_idx] = str(date_2k_mn_to_date(stream_item[date_crawled_idx]))
    attributes.update({i[0]: value for i, value in izip(STREAMS_HEADERS['INFOS'], stream_item)})


def extract_contents(attributes, stream_item):
    print stream_item
    content_type_id = stream_item[idx_from_stream('contents', 'content_type')]
    print 'has ctid', content_type_id
    txt = stream_item[idx_from_stream('contents', 'txt')]
    if "metadata" not in attributes:
        attributes["metadata"] = {}

    verbose_content_type = CONTENT_TYPE_INDEX[content_type_id]
    if verbose_content_type not in attributes["metadata"]:
        attributes["metadata"][verbose_content_type] = [txt]
    else:
        attributes["metadata"][verbose_content_type].append(txt)


def extract_outlinks(attributes, stream_item):
    link_type, follow, url_src, url_dst, external_url = stream_item
    if link_type == "a":
        location_key = "internal" if url_dst > 0 else "external"
        follow_key = "follow" if follow else "nofollow"
        key_nb = "outlinks_%s_%s_nb" % (location_key, follow_key)

        if key_nb not in attributes:
            attributes[key_nb] = 1
        else:
            attributes[key_nb] += 1

        if url_dst > 0:
            key_ids = "outlinks_%s_ids" % follow_key
            if key_ids not in attributes:
                attributes[key_ids] = [url_dst]
            else:
                attributes[key_ids].append(url_dst)
    elif link_type.startswith('r'):
        http_code = int(link_type[1:])
        attributes['redirect_to'] = {'url_id': url_dst, 'http_code': http_code}


def extract_inlinks(attributes, stream_item):
    link_type, follow, url_dst, url_src = stream_item
    if link_type == "a":
        follow_key = "follow" if follow else "nofollow"
        key_nb = "inlinks_%s_nb" % follow_key

        if key_nb not in attributes:
            attributes[key_nb] = 1
        else:
            attributes[key_nb] += 1

        if url_dst > 0:
            key_ids = "inlinks_%s_ids" % follow_key
            if key_ids not in attributes:
                attributes[key_ids] = [url_src]
            else:
                attributes[key_ids].append(url_src)
    elif link_type.startswith('r'):
        http_code = int(link_type[1:])
        attributes['redirect_from'] = {'url_id': url_src, 'http_code': http_code}


def extract_canonicals(attributes, stream_item):
    pass


class UrlsDocuments(object):
    EXTRACTORS = {
        'infos': extract_infos,
        'contents': extract_contents,
        'inlinks': extract_inlinks,
        'outlinks': extract_outlinks
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
