from __future__ import annotations

import os
from sqlite3 import OperationalError
from uuid import uuid4

import lazy_object_proxy
from diskcache import Cache

from simpleflow import constants, logger, storage
from simpleflow.settings import SIMPLEFLOW_ENABLE_DISK_CACHE
from simpleflow.utils import json_dumps, json_loads_or_raw

JUMBO_FIELDS_MEMORY_CACHE = {}


class JumboTooLargeError(ValueError):
    pass


def _jumbo_fields_bucket():
    # wrapped into a function so easier to override for tests
    bucket = os.getenv("SIMPLEFLOW_JUMBO_FIELDS_BUCKET")
    if not bucket:
        return
    # trim trailing / if there, would provoke double slashes down the road
    if bucket.endswith("/"):
        bucket = bucket[:-1]
    return bucket


def decode(content, parse_json=True, use_proxy=True):
    if content is None:
        return content
    if content.startswith(constants.JUMBO_FIELDS_PREFIX):

        def unwrap():
            location, _size = content.split()
            value = _pull_jumbo_field(location)
            if parse_json:
                return json_loads_or_raw(value)
            return value

        if use_proxy:
            return lazy_object_proxy.Proxy(unwrap)
        return unwrap()

    if parse_json:
        return json_loads_or_raw(content)

    return content


def encode(message, max_length, allow_jumbo_fields=True):
    if not message:
        return message

    can_use_jumbo_fields = allow_jumbo_fields and _jumbo_fields_bucket()

    if len(message) > max_length:
        if not can_use_jumbo_fields:
            _log_message_too_long(message)
            raise JumboTooLargeError(f"Message too long ({len(message)} chars)")

        if len(message) > constants.JUMBO_FIELDS_MAX_SIZE:
            _log_message_too_long(message)
            raise JumboTooLargeError("Message too long even for a jumbo field ({} chars)".format(len(message)))

        jumbo_signature = _push_jumbo_field(message)
        if len(jumbo_signature) > max_length:
            raise JumboTooLargeError(
                "Jumbo field signature is longer than the max allowed length "
                "for this field: {} ; reduce jumbo bucket length?".format(jumbo_signature)
            )
        return jumbo_signature

    return message


def _get_cached(path):
    # 1/ memory cache
    if path in JUMBO_FIELDS_MEMORY_CACHE:
        return JUMBO_FIELDS_MEMORY_CACHE[path]

    # 2/ disk cache
    if SIMPLEFLOW_ENABLE_DISK_CACHE:
        try:
            # NB: this cache may also be triggered on activity workers, where it's not that
            # useful. The performance hit should be minimal. To be improved later.
            # NB2: cache has to be lazily instantiated here, cache objects do not survive forks,
            # see DiskCache docs.
            cache = Cache(constants.CACHE_DIR)
            # generate a dedicated cache key because this cache may be shared with other
            # features of simpleflow at some point
            cache_key = "jumbo_fields/" + path.split("/")[-1]
            if cache_key in cache:
                logger.debug("diskcache: getting key={} from cache_dir={}".format(cache_key, constants.CACHE_DIR))
                return cache[cache_key]
        except OperationalError:
            logger.warning("diskcache: got an OperationalError, skipping cache usage")

    # nothing to return, but better be explicit here
    return


def _set_cached(path, content):
    # 1/ memory cache
    JUMBO_FIELDS_MEMORY_CACHE[path] = content

    # 2/ disk cache
    if SIMPLEFLOW_ENABLE_DISK_CACHE:
        try:
            cache = Cache(constants.CACHE_DIR)
            cache_key = "jumbo_fields/" + path.split("/")[-1]
            logger.debug("diskcache: setting key={} on cache_dir={}".format(cache_key, constants.CACHE_DIR))
            cache.set(cache_key, content, expire=3 * constants.HOUR)
        except OperationalError:
            logger.warning("diskcache: got an OperationalError on write, skipping cache write")


def _push_jumbo_field(message):
    size = len(message)
    uuid = str(uuid4())
    bucket_with_dir = _jumbo_fields_bucket()
    if "/" in bucket_with_dir:
        bucket, directory = _jumbo_fields_bucket().split("/", 1)
        path = f"{directory}/{uuid}"
    else:
        bucket = bucket_with_dir
        path = uuid

    storage.push_content(bucket, path, message)
    _set_cached(path, message)

    return f"{constants.JUMBO_FIELDS_PREFIX}{bucket}/{path} {size}"


def _pull_jumbo_field(location):
    bucket, path = location.replace(constants.JUMBO_FIELDS_PREFIX, "").split("/", 1)

    cached_value = _get_cached(path)
    if cached_value:
        return cached_value

    content = storage.pull_content(bucket, path)
    _set_cached(path, content)

    return content


def _log_message_too_long(message):
    if len(message) > constants.MAX_LOG_FIELD:
        message = "{} <...truncated to {} chars>".format(message[: constants.MAX_LOG_FIELD], constants.MAX_LOG_FIELD)
    logger.error(f"Message too long, will raise: {message}")


# A few helpers to wrap common SWF fields
def details(message):
    return encode(message, constants.MAX_DETAILS_LENGTH)


def execution_context(message):
    return encode(message, constants.MAX_EXECUTION_CONTEXT_LENGTH)


def heartbeat_details(message):
    return encode(message, constants.MAX_HEARTBEAT_DETAILS_LENGTH)


def identity(message):
    # we don't allow the use of jumbo fields for identity because it's guaranteed
    # to change on every task, and we fear it makes the decider too slow
    # NB: this should be revisited / questionned later, maybe not such a problem?
    return encode(message, constants.MAX_IDENTITY_LENGTH, allow_jumbo_fields=False)


def input(message):
    return encode(json_dumps(message), constants.MAX_INPUT_LENGTH)


def reason(message):
    return encode(message, constants.MAX_REASON_LENGTH)


def result(message):
    return encode(json_dumps(message), constants.MAX_RESULT_LENGTH)


def control(message):
    return encode(json_dumps(message), constants.MAX_CONTROL_LENGTH)
