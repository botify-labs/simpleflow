import gzip
import ujson


def extract_url_id(line):
    return line[0:line.index('\t')]


def clean_main_value(i, value):
    if UrlsDocuments.FIELDS_URLIDS[i] == 'id':
        return int(value)
    return value


def clean_infos_value(i, value):
    if UrlsDocuments.FIELDS_URLINFOS[i] in ('depth', 'http_code', 'byte_size', 'delay1', 'delay2'):
        return int(value)
    elif UrlsDocuments.FIELDS_URLINFOS[i] in ('gzipped', ):
        return bool(value)
    return value


class UrlsDocuments(object):
    FIELDS_URLIDS = ('id', 'protocol', 'host', 'path', 'query_string')
    FIELDS_URLINFOS = ('id', 'depth', 'date_crawled', 'http_code', 'byte_size', 'delay1', 'delay2', 'gzipped')

    CONTENT_TYPE_INDEX = {
        '1': 'title',
        '2': 'h1',
        '3': 'h2',
        '4': 'description'
    }

    def __init__(self, ids_file, infos_file, contents_file, debug=False):
        self.files = {}
        self.debug = debug
        for file_type, location in (('ids', ids_file), ('infos', infos_file), ('contents', contents_file)):
            if location.endswith('.gz'):
                open_func = gzip.open
            else:
                open_func = open
            self.files[file_type] = open_func(location)

    def next(self):
        line_content = self.files['contents'].next()[:-1]

        while True:
            line_url = self.files['ids'].next()[:-1]
            line_infos = self.files['infos'].next()[:-1]
            print line_url
            print line_infos
            # Be careful : it's a string (not an int) !
            current_url_id = extract_url_id(line_url)

            if self.debug:
                print 'current url id', current_url_id

            # Create initial dictionary
            url_data = {self.FIELDS_URLIDS[i]: clean_main_value(i, value) for i, value in enumerate(line_url.split('\t'))}

            # Update dict with infos
            url_data.update({self.FIELDS_URLINFOS[i]: clean_infos_value(i, value) for i, value in enumerate(line_infos.split('\t'))})

            while extract_url_id(line_content) == current_url_id:
                url_id, content_type_id, hash_id, txt = line_content.split('\t')
                content_type = self.CONTENT_TYPE_INDEX[content_type_id]
                if "metadata" not in url_data:
                    url_data["metadata"] = {}
                if content_type not in url_data["metadata"]:
                    url_data["metadata"][content_type] = []
                url_data["metadata"][content_type].append(txt)
                line_content = self.files['contents'].next()[:-1]

            yield url_data

    def save_to_file(self, location):
        for file_type in self.files.iterkeys():
            self.files[file_type].seek(0)
        f = open(location, 'w')
        f.writelines('\n'.join((ujson.encode(l) for l in self.next())))
        f.close()
