from itertools import izip

from cdf.streams.utils import iterate_csv_safely

"""
A stream is a generator of values. A value may be any object but usually is a
string or a tuple. The purpose of this module is to provide simple operations
that are easy to compose.

A stream allow to chain operations on each of its elements and consume the
values on-demand. Calling a stream operation after another does not iterate two
times on the values.

Example:

>>> import streams.utils
>>> INFOS_FIELDS = [('id', int),
...                 ('depth', int),
...                 ('date_crawled', None),
...                 ('http_code', int),
...                 ('byte_size', int),
...                 ('delay1', int),
...                 ('delay2', bool),
...                 ('gzipped', bool)]
>>> cast = Caster(INFOS_FIELDS).cast
>>> inlinks = cast(streams.utils.split_file((open('test.data'))))

"""

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
        for i in iterate_csv_safely(iterable):
            yield self.cast_line(i)
