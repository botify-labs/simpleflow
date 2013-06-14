from cdf.settings import STREAMS_HEADERS


"""
A stream is a generator of values. A value may be any object but usually is a
string or a tuple. The purpose of this module is to provide simple operations
that are easy to compose.

A stream allow to chain operations on each of its elements and consume the
values on-demand. Calling a stream operation after another does not iterate two
times on the values.

Example:

>>> import streams
>>> INFOS_FIELDS = [('id', int),
...                 ('depth', int),
...                 ('date_crawled', None),
...                 ('http_code', int),
...                 ('byte_size', int),
...                 ('delay1', int),
...                 ('delay2', bool),
...                 ('gzipped', bool)]
>>> cast = Caster(INFOS_FIELDS).cast
>>> inlinks = cast(streams.split_file((open('test.data'))))

"""
from itertools import izip

__all__ = ['split', 'rstrip', 'split_file', 'Caster']


class Caster(object):
    """
    Cast each field value to an object with respect to a definition mapping in
    *fields*.

    """
    def __init__(self, fields):
        self._fields = fields

    def cast_line(self, line):
        return [(cast(value) if cast else value) for
                (name, cast), value in izip(self.fields, line)]

    def cast(self, iterable):
        for i in iterable:
            yield self.cast_line(i)


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


"""
Return the field position of 'id' field from a specific stream
"""
def idx_from_stream(key, field):
    return map(lambda i: i[0], STREAMS_HEADERS[key.upper()]).index(field)
