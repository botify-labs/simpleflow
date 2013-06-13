from datetime import datetime, timedelta


class PatternSerializer(object):
    FIELDS = ('id', 'protocol', 'host', 'path', 'query_string')
    CTYPES = (int, str, str, str, str)

    """
    Deserialize from a string (fields have to be splited by a tab character)
    """
    @staticmethod
    def loads(line):
        return [PatternSerializer.CTYPES[i](value) for i, value in enumerate(line.split('\t'))]


class InfosSerializer(object):

    @staticmethod
    def to_date(date_str):
        return datetime(2000, 1, 1) + timedelta(minutes=int(date_str))

    FIELDS = ('id', 'depth', 'date_crawled', 'http_code', 'byte_size', 'delay1', 'delay2', 'gzipped')
    MAPPERS = (int, int, to_date, int, int, int, bool, bool)

    """
    Deserialize from a string (fields have to be splited by a tab character)
    """
    @staticmethod
    def loads(line):
        mapper = lambda i, v: InfosSerializer.MAPPERS[i](v) if InfosSerializer.MAPPERS[i] else v
        return [mapper(i, value) for i, value in enumerate(line.split('\t'))]


class ContentsSerializer(object):
    @staticmethod
    def to_content_type(ct_str):
        return ContentsSerializer.CONTENT_TYPE_INDEX[ct_str]

    FIELDS = ('id', 'content_type', 'hash', 'txt')
    CONTENT_TYPE_INDEX = {
        1: 'title',
        2: 'h1',
        3: 'h2',
        4: 'description'
    }
    MAPPERS = (int, to_content_type, None, None)

    """
    Deserialize from a string (fields have to be splited by a tab character)
    """
    @staticmethod
    def loads(line):
        mapper = lambda i, v: ContentsSerializer.MAPPERS[i](v) if InfosSerializer.MAPPERS[i] else v
        return [mapper(i, value) for i, value in enumerate(line.split('\t'))]
