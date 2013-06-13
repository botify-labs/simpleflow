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
    def to_date(str):
        return 5

    FIELDS = ('id', 'depth', 'date_crawled', 'http_code', 'byte_size', 'delay1', 'delay2', 'gzipped')
    MAPPERS = (int, int, to_date, int, int, int, bool, bool)

    """
    Deserialize from a string (fields have to be splited by a tab character)
    """
    @staticmethod
    def loads(line):
        mapper = lambda i, v: InfosSerializer.MAPPERS[i](v) if InfosSerializer.MAPPERS[i] else v
        return [mapper(i, value) for i, value in enumerate(line.split('\t'))]
