import logging
import os
from uuid import uuid4

from . import constants

from simpleflow import storage
from simpleflow.utils import json_dumps, json_loads_or_raw


logger = logging.getLogger(__name__)
JUMBO_FIELDS_BUCKET = os.getenv("SIMPLEFLOW_JUMBO_FIELDS_BUCKET")


def decode(content):
    if content.startswith(constants.JUMBO_FIELDS_PREFIX):
        location, _size = content.split()
        content = _pull_jumbo_field(location)
    return json_loads_or_raw(content)


def encode(message, max_length):
    if not message:
        return message

    if len(message) > max_length:
        if not JUMBO_FIELDS_BUCKET:
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
    bucket, directory = JUMBO_FIELDS_BUCKET.split("/", 1)
    path = "{}/{}".format(directory, uuid)
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
