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


def group_left(left, **stream_defs):
    """
    :param left: (stream, key_index)
    :param **stream_defs: {stream_name: (stream, key_index)

    :returns:
    :rtype: yield (key, left_line, {stream_name1: [stream_item_1, stream_itemX..], stream_name2: line_content})

    >>>> iter_streams(patterns=pattern_stream, infos=pattern_info)
    """
    id_ = {}
    right_line = {}
    left_stream, left_key_idx = left

    for line in left_stream:
        current_id = line[left_key_idx]
        stream_lines = {}

        for stream_name, stream_def in stream_defs.iteritems():
            stream, key_idx = stream_def
            if not stream_name in id_:
                right_line[stream_name] = stream.next()
                id_[stream_name] = right_line[stream_name][key_idx]

            while id_[stream_name] == current_id:
                try:
                    if stream_name in stream_lines:
                        stream_lines[stream_name].append(right_line[stream_name])
                    else:
                        stream_lines[stream_name] = [right_line[stream_name]]
                    right_line[stream_name] = stream.next()
                    id_[stream_name] = right_line[stream_name][key_idx]
                except StopIteration:
                    break
        yield current_id, line, stream_lines
