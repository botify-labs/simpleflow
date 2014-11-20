"""
A stream is a generator of values. A value may be any object but usually is a
string or a tuple. The purpose of this module is to provide simple operations
that are easy to compose.

A stream allow to chain operations on each of its elements and consume the
values on-demand. Calling a stream operation after another does not iterate two
times on the values.

"""

import os
from itertools import groupby
from cdf.utils import s3


__all__ = ['split', 'rstrip', 'split_file', 'get_data_streams_from_storage']


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


def group_left1(left, **stream_defs):
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
        stream_lines = {stream_name: [] for stream_name in stream_defs.iterkeys()}

        for stream_name, stream_def in stream_defs.iteritems():
            stream, key_idx = stream_def
            if stream_name in id_:
                if id_[stream_name] < current_id:
                    id_.pop(stream_name)

            if not stream_name in id_:
                try:
                    right_line[stream_name] = stream.next()
                except StopIteration:
                    continue
                id_[stream_name] = right_line[stream_name][key_idx]

            while id_[stream_name] == current_id:
                try:
                    stream_lines[stream_name].append(right_line[stream_name])
                    right_line[stream_name] = stream.next()
                    id_[stream_name] = right_line[stream_name][key_idx]
                except IOError, e:
                    raise Exception('IE Error on {} : {}'.format(stream_name, e))
                except StopIteration:
                    break
        yield current_id, line, stream_lines


def group_left(left, **streams):
    """Join multiple key-sorted streams together

    :param left: reference stream
    :type left: iterator
    :param streams: other streams
    :type streams: dict
    :return: grouped stream: (key, left_item, right_items)
    :rtype: (key_type, list, dict)
    """
    # grouped right streams by its key field
    grouped_streams = {
        name: groupby(stream, lambda l: l[idx])
        for name, (stream, idx) in streams.iteritems()
    }
    left_stream, left_key_idx = left

    last_buffer = {}

    for line in left_stream:
        # current id in left stream
        current_id = line[left_key_idx]
        # result buffer for right streams
        result_buffer = {name: [] for name in streams.iterkeys()}

        for stream_name, grouped_stream in grouped_streams.iteritems():
            # check if we've already get the id from the iterator
            if stream_name in last_buffer:
                key = last_buffer[stream_name][0]
                if key < current_id:
                    # remove it
                    last_buffer.pop(stream_name)
                elif key == current_id:
                    result_buffer[stream_name] = list(
                        last_buffer.pop(stream_name)[1])
                    continue
                else:
                    # if key is still bigger than the current_id
                    # no need to examine the stream, fast-forward to next
                    continue

            for key, group in grouped_stream:
                if key < current_id:
                    # ignore lines with non-existing id in left stream
                    continue
                elif key > current_id:
                    # potentially next ids, store it in the last buffer
                    last_buffer[stream_name] = (key, group)
                    break
                else:  # key is the current_id
                    result_buffer[stream_name] = list(group)
                    break

        yield current_id, line, result_buffer


def get_data_streams_from_storage(streams, storage_uri, tmp_dir, part_id=None, force_fetch=False):
    """
    :param streams : a StreamBase object
    :param storage_uri : an S3 uri
    :param tmp_dir : path location where to fetch files
    :param part_id : fetch from a specific part_id
    :param force_fetch : fetch the file even if already downloaded in tmp_dir
    """
    if part_id is not None:
        files = ['{}.txt.{}.gz'.format(s.FILE, part_id) for s in streams]
    else:
        files = ['{}.txt.([0-9]+).gz'.format(s.FILE) for s in streams]

    files_fetched = s3.fetch_files(storage_uri, tmp_dir,
                                   regexp=files,
                                   force_fetch=force_fetch)

    data_streams = []
    for path_local, fetched in files_fetched:
        for s in streams:
            if path_local.startswith(os.path.join(tmp_dir, "{}.txt".format(s.FILE))):
                data_streams.append(s.load_path(path_local))
    return data_streams


def identifier_to_stream_def_object(identifier):
    """
    Return a StreamDef from its file identifier
    """
    from cdf.core.features import Feature
    for f in Feature.get_all():
        for s in f.get_streams_objects:
            if s.FILE == identifier:
                return s
