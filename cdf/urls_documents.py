import ujson

from cdf.log import logger
from cdf.serializers import PatternSerializer, InfosSerializer


def clean_infos_value(i, value):
    if InfosSerializer.FIELDS[i] == 'date_crawled':
        return str(value)
    return value


def update_document_patterns(url_data, stream_item):
    # Create initial dictionary
    url_data.update({PatternSerializer.FIELDS[i]: value for i, value in enumerate(stream_item)})

    # query_string fields
    query_string = stream_item[4]
    if query_string:
        # The first character is ? we flush it in the split
        qs = [k.split('=') if '=' in k else [k, ''] for k in query_string[1:].split('&')]
        url_data['query_string_keys'] = [q[0] for q in qs]
        url_data['query_string_keys_order'] = ';'.join(url_data['query_string_keys'])
        url_data['query_string_items'] = qs


def update_document_infos(url_data, stream_item):
    url_data.update({InfosSerializer.FIELDS[i]: clean_infos_value(i, value) for i, value in enumerate(stream_item)})


def update_document_contents(url_data, stream_item):
    url_id, content_type, hash_id, txt = stream_item
    if "metadata" not in url_data:
        url_data["metadata"] = {}
    if content_type not in url_data["metadata"]:
        url_data["metadata"][content_type] = []
    url_data["metadata"][content_type].append(txt)


def update_document_outlinks(url_data, stream_item):
    link_type, follow, url_src, url_dst, external_url = stream_item
    if link_type == "a":
        location_key = "internal" if url_dst > 0 else "external"
        follow_key = "follow" if follow else "nofollow"
        key_nb = "outlinks_%s_%s_nb" % (location_key, follow_key)

        if key_nb not in url_data:
            url_data[key_nb] = 1
        else:
            url_data[key_nb] += 1

        if url_dst > 0:
            key_ids = "outlinks_%s_ids" % follow_key
            if key_ids not in url_data:
                url_data[key_ids] = [url_dst]
            else:
                url_data[key_ids].append(url_dst)
    elif link_type.startswith('r'):
        http_code = int(link_type[1:])
        url_data['redirect_to'] = {'url_id': url_dst, 'http_code': http_code}


def update_document_inlinks(url_data, stream_item):
    link_type, follow, url_dst, url_src = stream_item
    if link_type == "a":
        follow_key = "follow" if follow else "nofollow"
        key_nb = "inlinks_%s_nb" % follow_key

        if key_nb not in url_data:
            url_data[key_nb] = 1
        else:
            url_data[key_nb] += 1

        if url_dst > 0:
            key_ids = "inlinks_%s_ids" % follow_key
            if key_ids not in url_data:
                url_data[key_ids] = [url_src]
            else:
                url_data[key_ids].append(url_src)
    elif link_type.startswith('r'):
        http_code = int(link_type[1:])
        url_data['redirect_from'] = {'url_id': url_src, 'http_code': http_code}


class UrlsDocuments(object):
    STREAMS_MAPPERS = {
        'patterns': {
            'func': update_document_patterns,
            'url_id_index': 0,
        },
        'infos': {
            'func': update_document_infos,
            'url_id_index': 0,
        },
        'outlinks': {
            'func': update_document_outlinks,
            'url_id_index': 2
        },
        'inlinks': {
            'func': update_document_inlinks,
            'url_id_index': 2
        },
        'contents': {
            'func': update_document_contents,
            'url_id_index': 0
        }
    }

    def __init__(self, stream_patterns, stream_infos, stream_contents, stream_outlinks, stream_inlinks):
        self.streams = {
            'patterns': stream_patterns,
            'infos': stream_infos,
            'contents': stream_contents,
            'outlinks': stream_outlinks,
            'inlinks': stream_inlinks,
        }

        self.current_line = {}
        for stream_type in self.streams.keys():
            try:
                self.current_line[stream_type] = next(self.streams[stream_type])
            except StopIteration:
                self.current_line[stream_type] = None


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
        while True:
            if not self.current_line['patterns']:
                break

            url_data = {}
            current_url_id = self.current_line['patterns'][0]
            logger.info('current url_id : %s' % current_url_id)

            # Join others streams
            for stream_type, config in self.STREAMS_MAPPERS.iteritems():
                if self.current_line[stream_type]:
                    while self.current_line[stream_type][config['url_id_index']] == current_url_id:
                        config['func'](url_data, self.current_line[stream_type])
                        try:
                            self.current_line[stream_type] = next(self.streams[stream_type])
                        except StopIteration:
                            self.current_line[stream_type] = None
                            break

            yield url_data

    def save_to_file(self, location):
        for file_type in self.files.iterkeys():
            self.files[file_type].seek(0)
        f = open(location, 'w')
        f.writelines('\n'.join((ujson.encode(l) for l in self)))
        f.close()
