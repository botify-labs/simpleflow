from boto.s3 import connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from . import settings

try:
    from urlparse import urlparse
except:
    from urllib.parse import urlparse


BUCKET_CACHE = {}


def get_connection(host):
    return connection.S3Connection(host=host)


def sanitize_bucket_and_host(bucket):
    """
    if bucket is in following format : 'xxx.amazonaws.com/bucket_name',
    Returns a 2-values tuple ('bucket_name', 'xxx.amazonaws.com')
    """
    if "/" in bucket:
        host, bucket = bucket.split('/')
        if not host.endswith('amazonaws.com'):
            raise ValueError('host is waiting for an *.amazonaws.com URL')
        return (bucket, host)
    return (bucket, settings.SIMPLEFLOW_S3_HOST)


def get_bucket(bucket):
    bucket, host = sanitize_bucket_and_host(bucket)
    connection = get_connection(host)
    if not bucket in BUCKET_CACHE:
        BUCKET_CACHE[bucket] = connection.get_bucket(bucket)
    return BUCKET_CACHE[bucket]

def pull(bucket, path, dest_file):
    bucket = get_bucket(bucket)
    key = bucket.get_key(path)
    key.get_contents_to_filename(dest_file)


def pull_content(bucket, path):
    bucket = get_bucket(bucket)
    key = bucket.get_key(path)
    return key.get_contents_as_string(encoding='utf-8')


def push(bucket, path, src_file, content_type=None):
    bucket = get_bucket(bucket)
    key = Key(bucket, path)
    headers = {}
    if content_type:
        headers["content_type"] = content_type
    key.set_contents_from_filename(src_file, headers=headers)


def push_content(bucket, path, content, content_type=None):
    bucket = get_bucket(bucket)
    key = Key(bucket, path)
    headers = {}
    if content_type:
        headers["content_type"] = content_type
    key.set_contents_from_string(content, headers=headers)


def list_keys(bucket, path=None):
    bucket = get_bucket(bucket)
    return bucket.list(path)
