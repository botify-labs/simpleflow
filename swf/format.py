from . import constants
import logging


logger = logging.getLogger(__name__)


def wrap(message, max_length):
    if not message:
        return message

    if len(message) > max_length:
        logger.warning(
            'message "{}" too long ({} chars), wrapped to {}'.format(
                message,
                len(message),
                max_length,
            ))
        return message[:max_length]

    return message


def details(message):
    return wrap(message, constants.MAX_DETAILS_LENGTH)


def execution_context(message):
    return wrap(message, constants.MAX_EXECUTION_CONTEXT_LENGTH)


def heartbeat_details(message):
    return wrap(message, constants.MAX_HEARTBEAT_DETAILS_LENGTH)


def identity(message):
    return wrap(message, constants.MAX_IDENTITY_LENGTH)


def input(message):
    return wrap(message, constants.MAX_INPUT_LENGTH)


def reason(message):
    return wrap(message, constants.MAX_REASON_LENGTH)


def result(message):
    return wrap(message, constants.MAX_RESULT_LENGTH)
