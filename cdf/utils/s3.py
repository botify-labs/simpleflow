import time
import os
import re
from urlparse import urlparse
import zlib
import boto
from boto.s3.key import Key
from cdf.log import logger
from cdf.utils.path import makedirs, partition_aware_sort

from lockfile import FileLock
from threading import Lock


class Connection(object):
    """A class for S3 connection
    The purpose of this class is to contain
    a global boto connection object
    that is instanciated only when needed.
    """
    #the actual connection
    #it is lazily instanciated
    _conn = None
    _lock = Lock()

    @staticmethod
    def get():
        """A getter for the connection"""
        with Connection._lock:
            if not Connection._conn:
                Connection._conn = boto.connect_s3()
        return Connection._conn


def uri_parse(s3_uri):
    """
    Return a tuple (bucket_name, location)
    from an s3_uri with the following scheme:
        s3://bucket/location
    """
    p = urlparse(s3_uri)
    if not p.scheme == 's3':
        raise Exception('Protocol should be `s3`')
    return (p.netloc, p.path[1:])


def list_files(s3_uri, regexp=None):
    """Return list of `boto.s3.Key` objects

    It does not support recursive listing.

    :param s3_uri: s3 path that contains the keys
    :param regexp: regexp used to filter the basename of s3 keys
    """
    bucket, location = uri_parse(s3_uri)
    bucket = Connection.get().get_bucket(bucket)
    files = []

    for key_obj in bucket.list(prefix=location):
        key = key_obj.name
        key_basename = os.path.basename(key)

        if (not regexp
            or (isinstance(regexp, str) and re.match(regexp, key_basename))
            or (isinstance(regexp, (list, tuple))
                and any(re.match(r, key_basename) for r in regexp))):
            files.append(key_obj)
    return files


def fetch_files(s3_uri, dest_dir, regexp=None, force_fetch=True, lock=True):
    """
    Fetch files from an `s3_uri` and save them to `dest_dir`
    Files can be filters by a list of `prefixes` or `suffixes`
    If `force_fetch` is False, files will be fetched only if the file is not existing in the dest_dir

    Return a list of tuples (local_path, fetched) where `fetched` is a boolean
    """
    bucket, location = uri_parse(s3_uri)
    files = []

    for key_obj in list_files(s3_uri, regexp):
        key = key_obj.name
        key_basename = os.path.basename(key)

        path = os.path.join(dest_dir, key_basename)

        if lock:
            lock_obj = FileLock(path)

        makedirs(os.path.dirname(path), exist_ok=True)
        if not force_fetch and os.path.exists(path):
            if lock:
                nb_checks = 0
                while lock_obj.is_locked():
                    time.sleep(1)
                    nb_checks += 1
                    if nb_checks > 10:
                        raise Exception('Timeout on lock checking for %s' % path)
            files.append((path, False))
            continue
        logger.info('Fetch %s' % key)

        if lock:
            with lock_obj:
                key_obj.get_contents_to_filename(path)
        else:
            key_obj.get_contents_to_filename(path)

        files.append((path, True))
    return files


def fetch_file(s3_uri, dest_dir, force_fetch, lock=True):
    if not force_fetch and os.path.exists(dest_dir):
        return (dest_dir, False)
    key_obj = get_key_from_s3_uri(s3_uri)

    # If the file does not exist, a `boto.exception.S3ResponseError`
    # will be raised when calling `get_contents_to_filename`
    makedirs(os.path.dirname(dest_dir), exist_ok=True)
    logger.info('Fetch %s to %s' % (s3_uri, dest_dir))
    if lock:
        lock_obj = FileLock(dest_dir)
        with lock_obj:
            key_obj.get_contents_to_filename(dest_dir)
    else:
        key_obj.get_contents_to_filename(dest_dir)
    return (dest_dir, True)


def _split_by_lines(text_stream):
    """Split text stream into lines

    :param text_stream: text stream
    :return: a generator function of lines
    """
    last_line = ''
    try:
        while True:
            chunk = last_line + next(text_stream)
            chunk_by_line = chunk.split('\n')
            last_line = chunk_by_line.pop()
            for line in chunk_by_line:
                yield line + '\n'
    except StopIteration:  # the other end of the pipe is empty
        if len(last_line) > 0:
            yield last_line + '\n'
        raise StopIteration


def _stream_decompress(stream):
    """Decompress a gzipped stream

    Credit: http://stackoverflow.com/questions/12571913
    """
    dec = zlib.decompressobj(16+zlib.MAX_WBITS)  # same as gzip module
    for chunk in stream:
        rv = dec.decompress(chunk)
        if rv:
            yield rv


def stream_files(s3_uri, regexp=None,
                 stream_func=_stream_decompress):
    """Stream S3 files

    Partition files' order is respected.
    A byte stream manipulation function can be specified
    (eg. zlib decompression function to decompress gzip stream)

    :param s3_uri: s3 uri to files
    :param regexp: optional regexp to filter files
    :param stream_func: a generator function that manipulates the
        byte stream, defaults to zlib stream decompression function
    :return: a generator of text line streams
    """
    keys = list_files(s3_uri, regexp=regexp)
    basename_func = lambda i: os.path.basename(i.name)
    # iterate sorted keys
    for key in partition_aware_sort(keys, basename_func=basename_func):
        # stream each key
        for line in _split_by_lines(stream_func(key)):
            yield line


def get_key_from_s3_uri(s3_uri):
    bucket, location = uri_parse(s3_uri)
    bucket = Connection.get().get_bucket(bucket)
    return Key(bucket, location)


def push_content(s3_uri, content, content_type=None):
    key = get_key_from_s3_uri(s3_uri)
    if content_type:
        key.content_type = content_type
    key.set_contents_from_string(content)


def push_file(s3_uri, filename):
    key = get_key_from_s3_uri(s3_uri)
    logger.info("Push {}".format(s3_uri))
    key.set_contents_from_filename(filename)
