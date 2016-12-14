from boto.s3 import connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket


BUCKET_CACHE = {}


def get_connection(region):
    return connection.S3Connection(region)


def get_bucket(bucket, region='us-east-1'):
    if not bucket in BUCKET_CACHE:
        connection = get_connection(region)
        BUCKET_CACHE[bucket] = connection.get_bucket(bucket)
    return BUCKET_CACHE[bucket]


def pull(bucket, path, dest_file, region='us-east-1'):
    bucket = get_bucket(bucket, region)
    key = bucket.get_key(path)
    key.get_contents_to_filename(dest_file)


def pull_content(bucket, path, region='us-east-1'):
    bucket = get_bucket(bucket, region)
    key = bucket.get_key(path)
    return key.get_contents_as_string()


def push(bucket, path, src_file, region='us-east-1'):
    bucket = get_bucket(bucket, region)
    key = Key(bucket, path)
    key.set_contents_from_filename(src_file)


def push_content(bucket, path, content, region='us-east-1'):
    bucket = get_bucket(bucket, region)
    key = Key(bucket, path)
    key.set_contents_from_string(content)
