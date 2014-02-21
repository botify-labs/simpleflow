"""
A stream is a generator of values. A value may be any object but usually is a
string or a tuple. The purpose of this module is to provide simple operations
that are easy to compose.

A stream allow to chain operations on each of its elements and consume the
values on-demand. Calling a stream operation after another does not iterate two
times on the values.

Example:

>>> from cdf.core.streams.utils import split_file
>>> INFOS_FIELDS = [('id', int),
...                 ('depth', int),
...                 ('date_crawled', None),
...                 ('http_code', int),
...                 ('byte_size', int),
...                 ('delay1', int),
...                 ('delay2', bool),
...                 ('gzipped', bool)]
>>> cast = Caster(INFOS_FIELDS).cast
>>> inlinks = cast(split_file((open('test.data'))))
"""
from itertools import izip

__all__ = ['Caster']


class Caster(object):
    """
    Cast each field value to an object with respect to a definition mapping in
    *fields*.

    """
    def __init__(self, fields):
        self._fields = fields

    def cast_line(self, line):
        return [(cast(value) if cast else value) for
                (name, cast), value in izip(self._fields, line)]

    def cast(self, iterable):
        for i in iterable:
            yield self.cast_line(i)


