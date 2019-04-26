from typing import TYPE_CHECKING

from boto.s3 import connect_to_region, connection
from boto.s3.key import Key
from boto.exception import S3ResponseError

from . import logger, settings

if TYPE_CHECKING:
    from typing import Optional, Tuple  # NOQA
    from boto.s3.bucket import Bucket  # NOQA
    from boto.s3.bucketlistresultset import BucketListResultSet  # NOQA

BUCKET_CACHE = {}
BUCKET_LOCATIONS_CACHE = {}


def get_connection(host_or_region):
    # type: (str) -> connection.S3Connection
    # first case: we got a valid DNS (host)
    if "." in host_or_region:
        return connection.S3Connection(host=host_or_region)

    # second case: we got a region
    return connect_to_region(host_or_region)


def sanitize_bucket_and_host(bucket):
    # type: (str) -> Tuple[str, str]
    """
    if bucket is in following format : 'xxx.amazonaws.com/bucket_name',
    Returns a 2-values tuple ('bucket_name', 'xxx.amazonaws.com')
    """
    # first case: we got a "<host>/<bucket_name>" input
    if "/" in bucket:
        host, bucket = bucket.split('/', 1)
        if "/" in bucket:
            raise ValueError('{} should contains only one slash separator'.format(bucket))
        if not host.endswith('amazonaws.com'):
            raise ValueError('host should be a *.amazonaws.com URL')
        return bucket, host

    # return location from cache is possible, so we don't issue "GetBucketLocation"
    # calls with each other S3 call
    if bucket in BUCKET_LOCATIONS_CACHE:
        return bucket, BUCKET_LOCATIONS_CACHE[bucket]

    # second case: we got a bucket name, we need to figure out which region it's in
    try:
        conn0 = connection.S3Connection()
        bucket_obj = conn0.get_bucket(bucket, validate=False)

        # get_location() returns a region or an empty string for us-east-1,
        # historically named "US Standard" in some places. Maybe other S3
        # calls support an empty string as region, but I prefer to be
        # explicit here.
        location = bucket_obj.get_location() or "us-east-1"

        # save location for later use
        BUCKET_LOCATIONS_CACHE[bucket] = location
    except S3ResponseError as e:
        if e.error_code == "AccessDenied":
            # probably not allowed to perform GetBucketLocation on this bucket
            logger.warning("Access denied while trying to get location of bucket {}".format(bucket))
            location = ""
        else:
            raise

    # fallback for backward compatibility
    if not location:
        location = settings.SIMPLEFLOW_S3_HOST

    return bucket, location


def get_bucket(bucket_name):
    # type: (str) -> Bucket
    bucket_name, location = sanitize_bucket_and_host(bucket_name)
    conn = get_connection(location)
    if bucket_name not in BUCKET_CACHE:
        bucket = conn.get_bucket(bucket_name, validate=False)
        BUCKET_CACHE[bucket_name] = bucket
    return BUCKET_CACHE[bucket_name]


def pull(bucket, path, dest_file):
    # type: (str, str, str) -> None
    bucket = get_bucket(bucket)
    key = bucket.get_key(path)
    key.get_contents_to_filename(dest_file)


def pull_content(bucket, path):
    # type: (str, str) -> str
    bucket = get_bucket(bucket)
    key = bucket.get_key(path)
    return key.get_contents_as_string(encoding='utf-8')


def push(bucket, path, src_file, content_type=None):
    # type: (str, str, str, Optional[str]) -> None
    bucket = get_bucket(bucket)
    key = Key(bucket, path)
    headers = {}
    if content_type:
        headers["content_type"] = content_type
    key.set_contents_from_filename(src_file, headers=headers, encrypt_key=settings.SIMPLEFLOW_S3_SSE)


def push_content(bucket, path, content, content_type=None):
    # type: (str, str, str, Optional[str]) -> None
    bucket = get_bucket(bucket)
    key = Key(bucket, path)
    headers = {}
    if content_type:
        headers["content_type"] = content_type
    key.set_contents_from_string(content, headers=headers, encrypt_key=settings.SIMPLEFLOW_S3_SSE)


def list_keys(bucket, path=None):
    # type: (str, str) -> BucketListResultSet
    bucket = get_bucket(bucket)
    return bucket.list(path)
