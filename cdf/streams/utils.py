"""
A stream is a generator of values. A value may be any object but usually is a
string or a tuple. The purpose of this module is to provide simple operations
that are easy to compose.

A stream allow to chain operations on each of its elements and consume the
values on-demand. Calling a stream operation after another does not iterate two
times on the values.

"""
from itertools import izip

from cdf.settings import STREAMS_HEADERS


__all__ = ['split', 'rstrip', 'split_file', 'idx_from_stream']


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
        yield i.rstrip('\n')


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


def idx_from_stream(key, field):
    """
    Return the field position of 'id' field from a specific stream

    :param key: stream key
    :field field name from stream

    """
    return map(lambda i: i[0], STREAMS_HEADERS[key.upper()]).index(field)
