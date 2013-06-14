"""
A stream is a generator of values. A value may be any object but usually is a
string or a tuple. The purpose of this module is to provide simple operations
that are easy to compose.

A stream allow to chain operations on each of its elements and consume the
values on-demand. Calling a stream operation after another does not iterate two
times on the values.

Example:

The code below transform a generator of line (`StringIO`) into a generator of
list where each element has the type defined by `INFOS_FIELDS`.

>>> INFOS_FIELDS = [('id', int),
...                 ('depth', int),
...                 ('date_crawled', None),
...                 ('http_code', int),
...                 ('byte_size', int),
...                 ('delay1', int),
...                 ('delay2', bool),
...                 ('gzipped', bool)]
>>> cast = Caster(INFOS_FIELDS).cast
>>> from StringIO import StringIO
>>> filep = StringIO('a\\tb\\tc\\n')
>>> cast(split_file(filep)).next()
Traceback (most recent call last):
    ...
ValueError: invalid literal for int() with base 10: 'a'
>>> filep = StringIO('0\\t1\\tdate\\n')
>>> cast(split_file(filep)).next()
[0, 1, 'date']

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
        """
        Example:

        >>> cast_line = Caster([('a', int), ('b', None)]).cast
        >>> cast_line(iter([('a', 'b')])).next()
        Traceback (most recent call last):
            ...
        ValueError: invalid literal for int() with base 10: 'a'
        >>> cast_line(iter([(1, 'b')])).next()
        [1, 'b']

        """
        return [(cast(value) if cast else value) for
                (name, cast), value in izip(self._fields, line)]

    def cast(self, iterable):
        for i in iterable:
            yield self.cast_line(i)


def split(iterable, char='\t'):
    """
    Split each line with *char*.

    Example:

    >>> split(iter(['a\\tb\\tc'])).next()
    ['a', 'b', 'c']
    >>> split(iter(['a,b,c']), ',').next()
    ['a', 'b', 'c']

    """
    return (i.split(char) for i in iterable)


def rstrip(iterable):
    """
    Strip end-of-line and trailing spaces.

    Example:

    >>> iterable = iter(['foo bar \\n'])
    >>> rstrip(iterable).next()
    'foo bar'

    """
    for i in iterable:
        yield i.rstrip()


def split_file(iterable, char='\t'):
    """
    Strip end-of-line and trailing spaces, then split each line with *char*.

    :param iterable: usually a file objects

    Example:

    >>> iterable = iter(['a\\tb\\tc  \\n'])
    >>> split_file(iterable).next()
    ['a', 'b', 'c']

    """
    return split(rstrip(iterable))
