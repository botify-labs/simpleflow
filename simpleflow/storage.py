from boto.s3 import connection
from boto.s3.key import Key

from . import settings


BUCKET_CACHE = {}


def get_connection(host):
    return connection.S3Connection(host=host)


def sanitize_bucket_and_host(bucket):
    """
    if bucket is in following format : 'xxx.amazonaws.com/bucket_name',
    Returns a 2-values tuple ('bucket_name', 'xxx.amazonaws.com')
    """
    if "/" in bucket:
        host, bucket = bucket.split('/', 1)
        if "/" in bucket:
            raise ValueError('{} should contains only one slash separator'.format(bucket))
        if not host.endswith('amazonaws.com'):
            raise ValueError('host should be a *.amazonaws.com URL')
        return bucket, host
    return bucket, settings.SIMPLEFLOW_S3_HOST


def get_bucket(bucket):
    bucket, host = sanitize_bucket_and_host(bucket)
    conn = get_connection(host)
    if bucket not in BUCKET_CACHE:
        bucket = conn.get_bucket(bucket)
        BUCKET_CACHE[bucket] = bucket
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
