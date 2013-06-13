import ujson

from cdf.log import logger
from cdf.serializers import PatternSerializer, InfosSerializer


def clean_infos_value(i, value):
    if InfosSerializer.FIELDS[i] == 'date_crawled':
        return str(value)
    return value


class UrlsDocuments(object):
    CONTENT_TYPE_INDEX = {
        '1': 'title',
        '2': 'h1',
        '3': 'h2',
        '4': 'description'
    }

    def __init__(self, stream_patterns, stream_infos, stream_contents):
        self.streams = {
            'patterns': stream_patterns,
            'infos': stream_infos,
            'contents': stream_contents
        }

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
            "bytesize": 14554,
            "outlinks_nb": 5,
            "inlinks_nb": 100,
            "outlinks_follow_nb": 4,
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
        try:
            line_content = next(self.streams['contents'])
        except StopIteration:
            line_content = None

        while True:
            try:
                line_url = next(self.streams['patterns'])
            except StopIteration:
                break
            line_infos = next(self.streams['infos'])

            current_url_id = line_url[0]
            logger.info('current url_id : %s' % current_url_id)

            # Create initial dictionary
            url_data = {PatternSerializer.FIELDS[i]: value for i, value in enumerate(line_url)}

            # query_string fields
            query_string = line_url[4]
            if query_string:
                # The first character is ? we flush it in the split
                qs = [k.split('=') if '=' in k else [k, ''] for k in query_string[1:].split('&')]
                url_data['query_string_keys'] = [q[0] for q in qs]
                url_data['query_string_keys_order'] = ';'.join(url_data['query_string_keys'])
                url_data['query_string_items'] = qs

            # Update dict with infos
            url_data.update({InfosSerializer.FIELDS[i]: clean_infos_value(i, value) for i, value in enumerate(line_infos)})

            if line_content:
                while line_content[0] == current_url_id:
                    url_id, content_type_id, hash_id, txt = line_content
                    content_type = self.CONTENT_TYPE_INDEX[content_type_id]
                    if "metadata" not in url_data:
                        url_data["metadata"] = {}
                    if content_type not in url_data["metadata"]:
                        url_data["metadata"][content_type] = []
                    url_data["metadata"][content_type].append(txt)
                    line_content = next(self.streams['contents'])

            yield url_data

    def save_to_file(self, location):
        for file_type in self.files.iterkeys():
            self.files[file_type].seek(0)
        f = open(location, 'w')
        f.writelines('\n'.join((ujson.encode(l) for l in self.next())))
        f.close()
