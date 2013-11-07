import time
import os
import re
from urlparse import urlparse

import boto
from boto.s3.key import Key
from cdf.log import logger
from cdf.utils.path import makedirs

from lockfile import FileLock

conn = boto.connect_s3()


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
    """
    Return list of boto.s3.Key objects
    """
    bucket, location = uri_parse(s3_uri)
    bucket = conn.get_bucket(bucket)
    files = []

    for key_obj in bucket.list(prefix=location):
        key = key_obj.name
        key_without_location = key[len(location) + 1:]

        if not regexp \
            or (isinstance(regexp, str) and re.match(regexp, key_without_location))\
                or (isinstance(regexp, (list, tuple)) and any(re.match(r, key_without_location) for r in regexp)):
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

        path = os.path.join(dest_dir, key[len(location) + 1:])

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
    """
    If the file does not exist, a `boto.exception.S3ResponseError`
    will be raised when calling `get_contents_to_filename`
    """
    logger.info('Fetch %s' % s3_uri)
    if lock:
        lock_obj = FileLock(dest_dir)
        with lock_obj:
            key_obj.get_contents_to_filename(dest_dir)
    else:
        key_obj.get_contents_to_filename(dest_dir)
    return (dest_dir, True)


def get_key_from_s3_uri(s3_uri):
    bucket, location = uri_parse(s3_uri)
    bucket = conn.get_bucket(bucket)
    key = Key(bucket, location)
    return key


def push_content(s3_uri, content):
    key = get_key_from_s3_uri(s3_uri)
    key.set_contents_from_string(content)


def push_file(s3_uri, filename):
    key = get_key_from_s3_uri(s3_uri)
    logger.info("Push {}".format(s3_uri))
    key.set_contents_from_filename(filename)
