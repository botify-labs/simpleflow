import logging
import os
from uuid import uuid4

import lazy_object_proxy

from . import constants

from simpleflow import storage
from simpleflow.utils import json_dumps, json_loads_or_raw


logger = logging.getLogger(__name__)


def _jumbo_fields_bucket():
    # wrapped into a function so easier to override for tests
    bucket = os.getenv("SIMPLEFLOW_JUMBO_FIELDS_BUCKET")
    if not bucket:
        return
    # trim trailing / if there, would provoke double slashes down the road
    if bucket.endswith("/"):
        bucket = bucket[:-1]
    return bucket


def decode(content):
    if content is None:
        return content
    if content.startswith(constants.JUMBO_FIELDS_PREFIX):

        def unwrap():
            location, _size = content.split()
            value = _pull_jumbo_field(location)
            return json_loads_or_raw(value)

        return lazy_object_proxy.Proxy(unwrap)
    return json_loads_or_raw(content)


def encode(message, max_length):
    if not message:
        return message

    if len(message) > max_length:
        if not _jumbo_fields_bucket():
            logger.warning(
                'message "{}" too long ({} chars), wrapped to {}'.format(
                    message,
                    len(message),
                    max_length,
                ))
            return message[:max_length]

        if len(message) > constants.JUMBO_FIELDS_MAX_SIZE:
            logger.warning(
                'message too long even for a jumbo field ({} chars), wrapped to {}'.format(
                    len(message),
                    constants.JUMBO_FIELDS_MAX_SIZE,
                ))
            message = message[:constants.JUMBO_FIELDS_MAX_SIZE]

        return _push_jumbo_field(message)

    return message


def _push_jumbo_field(message):
    size = len(message)
    uuid = str(uuid4())
    bucket_with_dir = _jumbo_fields_bucket()
    if "/" in bucket_with_dir:
        bucket, directory = _jumbo_fields_bucket().split("/", 1)
        path = "{}/{}".format(directory, uuid)
    else:
        bucket = bucket_with_dir
        path = uuid
    storage.push_content(bucket, path, message)
    return "{}{}/{} {}".format(constants.JUMBO_FIELDS_PREFIX, bucket, path, size)


def _pull_jumbo_field(location):
    bucket, path = location.replace(constants.JUMBO_FIELDS_PREFIX, "").split("/", 1)
    return storage.pull_content(bucket, path)


# A few helpers to wrap common SWF fields
def details(message):
    return encode(message, constants.MAX_DETAILS_LENGTH)


def execution_context(message):
    return encode(message, constants.MAX_EXECUTION_CONTEXT_LENGTH)


def heartbeat_details(message):
    return encode(message, constants.MAX_HEARTBEAT_DETAILS_LENGTH)


def identity(message):
    return encode(message, constants.MAX_IDENTITY_LENGTH)


def input(message):
    return encode(json_dumps(message), constants.MAX_INPUT_LENGTH)


def reason(message):
    return encode(message, constants.MAX_REASON_LENGTH)


def result(message):
    return encode(json_dumps(message), constants.MAX_RESULT_LENGTH)
