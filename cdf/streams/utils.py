from cdf.settings import STREAMS_HEADERS


__all__ = ['split', 'rstrip', 'split_file']


def split(iterable, char='\t'):
    """
    Split each line with *char*.

    """
    return (i.split(char) for i in iterable)


def rstrip(iterable):
    """
    Strip end-of-line and trailing spaces.

    """
    for i in iterable:
        yield i.rstrip()


def split_file(iterable, char='\t'):
    """
    Strip end-of-line and trailing spaces, then split each line with *char*.

    :param iterable: usually a file objects

    """
    return split(rstrip(iterable))


def idx_from_stream(key, field):
    """
    Return the field position of 'id' field from a specific stream

    :param key: stream key
    :field field name from stream

    """
    return map(lambda i: i[0], STREAMS_HEADERS[key.upper()]).index(field)
