import os
from urlparse import urlparse

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from cdf.log import logger

CONNECTIONS = dict()


def uri_parse(s3_uri):
    """
    Return a tuple (aws_access_key, aws_secret_key, bucket_name, location)
    from an s3_uri with the following scheme:
        s3://aws_access_key:aws_secret_key@bucket/location
    """
    p = urlparse(s3_uri)
    if not p.scheme == 's3':
        raise Exception('Protocol should be `s3`')
    credentials, bucket = p.netloc.split(':')
    access_key, secret_key = credentials.split('@')
    return (access_key, secret_key, bucket, p.path[1:])


def get_connection(access_key, secret_key):
    """
    Store connections to avoid created a new one at each call
    """
    key = (access_key, secret_key)
    if key not in CONNECTIONS:
        CONNECTIONS[key] = S3Connection(access_key, secret_key)
    return CONNECTIONS[key]


def fetch_files(s3_uri, dest_dir, prefixes=None, suffixes=None, force_fetch=True):
    """
    Fetch files from an `s3_uri` and save them to `dest_dir`
    Files can be filters by a list of `prefixes` or `suffixes`
    If `force_fetch` is False, files will be fetched only if the file is not existing in the dest_dir

    Return a list of tuples (local_path, fetched) where `fetched` is a boolean
    """
    access_key, secret_key, bucket, location = uri_parse(s3_uri)
    conn = get_connection(access_key, secret_key)
    bucket = conn.get_bucket(bucket)
    files = []

    for key_obj in bucket.list(prefix=location):
        key = key_obj.name
        key_without_location = key[len(location) + 1:]

        if (not prefixes or any(key_without_location.startswith(p) for p in prefixes)) and \
           (not suffixes or any(key_without_location.endswith(s) for s in suffixes)):
            path = os.path.join(dest_dir, key[len(location) + 1:])
            if not os.path.exists(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path))
            if not force_fetch and os.path.exists(path):
                files.append((path, False))
                continue
            logger.info('Fetch %s' % key)
            k = Key(bucket=bucket, name=key)
            k.get_contents_to_filename(path)
            files.append((path, True))
    return files


def push_content(s3_uri, content):
    access_key, secret_key, bucket, location = uri_parse(s3_uri)
    conn = get_connection(access_key, secret_key)
    bucket = conn.get_bucket(bucket)
    key = Key(bucket, location)
    key.set_contents_from_string(content)
