from boto.s3 import connection
from boto.s3.key import Key

from . import settings

BUCKET_CACHE = {}


def get_connection(host):
    return connection.S3Connection(host=host)


def get_bucket(bucket):
    if bucket not in BUCKET_CACHE:
        connection = get_connection(settings.SIMPLEFLOW_S3_HOST)
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
