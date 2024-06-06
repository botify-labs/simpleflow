from __future__ import annotations

import io
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError

from . import logger, settings
from .boto3_utils import get_or_create_boto3_client
from .swf.mapper.exceptions import extract_error_code

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Bucket, ObjectSummary

BUCKET_CACHE = {}
BUCKET_LOCATIONS_CACHE = {}


def get_client() -> boto3.session.Session.client:
    return get_or_create_boto3_client(region_name=None, service_name="s3")


def get_resource(host_or_region: str) -> boto3.session.Session.resource:
    # first case: we got a valid DNS (host)
    if "." in host_or_region:
        return boto3.resource("s3", endpoint_url=f"https://{host_or_region}")

    # second case: we got a region
    return boto3.resource("s3", region_name=host_or_region)


def sanitize_bucket_and_host(bucket: str) -> tuple[str, str]:
    """
    if bucket is in following format : 'xxx.amazonaws.com/bucket_name',
    Returns a 2-values tuple ('bucket_name', 'xxx.amazonaws.com')
    """
    # first case: we got a "<host>/<bucket_name>" input
    if "/" in bucket:
        host, bucket = bucket.split("/", 1)
        if "/" in bucket:
            raise ValueError(f"{bucket} should contains only one slash separator")
        if not host.endswith("amazonaws.com"):
            raise ValueError("host should be a *.amazonaws.com URL")
        return bucket, host

    # return location from cache is possible, so we don't issue "GetBucketLocation"
    # calls with each other S3 call
    if bucket in BUCKET_LOCATIONS_CACHE:
        return bucket, BUCKET_LOCATIONS_CACHE[bucket]

    # second case: we got a bucket name, we need to figure out which region it's in
    try:
        # get_bucket_location() returns a region or an empty string for us-east-1,
        # historically named "US Standard" in some places. Maybe other S3 calls
        # support an empty string as region, but I prefer to be explicit here.
        location = get_client().get_bucket_location(Bucket=bucket)["LocationConstraint"] or "us-east-1"

        # save location for later use
        BUCKET_LOCATIONS_CACHE[bucket] = location
    except ClientError as e:
        error_code = extract_error_code(e)
        if error_code == "AccessDenied":
            # probably not allowed to perform GetBucketLocation on this bucket
            # TODO: consider raising instead? who forbids GetBucketLocation anyway?
            logger.warning(f"Access denied while trying to get location of bucket {bucket}")
            location = ""
        else:
            raise

    # fallback for backward compatibility
    if not location:
        location = settings.SIMPLEFLOW_S3_HOST

    return bucket, location


def get_bucket(bucket_name: str) -> Bucket:
    bucket_name, location = sanitize_bucket_and_host(bucket_name)
    s3 = get_resource(location)
    if bucket_name not in BUCKET_CACHE:
        bucket = s3.Bucket(bucket_name)
        BUCKET_CACHE[bucket_name] = bucket
    return BUCKET_CACHE[bucket_name]


def pull(bucket: str, path: str, dest_file: str) -> None:
    bucket_resource = get_bucket(bucket)
    bucket_resource.download_file(path, dest_file)


def pull_content(bucket: str, path: str) -> str:
    bucket_resource = get_bucket(bucket)
    bytes_buffer = io.BytesIO()
    bucket_resource.download_fileobj(path, bytes_buffer)
    return bytes_buffer.getvalue().decode()


def push(bucket: str, path: str, src_file: str, content_type: str | None = None) -> None:
    bucket_resource = get_bucket(bucket)
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    if settings.SIMPLEFLOW_S3_SSE:
        extra_args["ServerSideEncryption"] = "AES256"
    bucket_resource.upload_file(src_file, path, ExtraArgs=extra_args)


def push_content(bucket: str, path: str, content: str, content_type: str | None = None) -> None:
    bucket_resource = get_bucket(bucket)
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    if settings.SIMPLEFLOW_S3_SSE:
        extra_args["ServerSideEncryption"] = "AES256"
    bucket_resource.upload_fileobj(io.BytesIO(content.encode()), path, ExtraArgs=extra_args)


def list_keys(bucket: str, path: str | None = None) -> list[ObjectSummary]:
    bucket_resource = get_bucket(bucket)
    return [obj for obj in bucket_resource.objects.filter(Prefix=path or "").all()]
