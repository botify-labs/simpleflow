class PatternSerializer(object):
    FIELDS = ('id', 'protocol', 'host', 'path', 'query_string')
    CTYPES = (int, str, str, str, str)

    """
    Deserialize from a string (fields have to be splited by a tab character)
    """
    @staticmethod
    def loads(line):
        return [PatternSerializer.CTYPES[i](value) for i, value in enumerate(line.split('\t'))]
