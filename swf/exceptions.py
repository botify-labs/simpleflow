# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from __future__ import annotations

import re
from collections.abc import Sequence
from functools import partial, wraps

import boto.swf.exceptions

from simpleflow import logger


class SWFError(Exception):
    def __init__(self, message, raw_error="", *args, **kwargs):
        """
        Examples:

        >>> error = SWFError('message')
        >>> error.message
        'message'
        >>> error.details
        ''
        >>> error = SWFError('message', 'kind')
        >>> error.message
        'message'
        >>> error.kind
        'kind'
        >>> error.details
        ''
        >>> error = SWFError('message', 'kind:')
        >>> error.message
        'message'
        >>> error.kind
        'kind'
        >>> error.details
        ''
        >>> error = SWFError('message', 'kind:details')
        >>> error.message
        'message'
        >>> error.kind
        'kind'
        >>> error.details
        'details'
        >>> error = SWFError('message', 'kind:  details ')
        >>> error.message
        'message'
        >>> error.kind
        'kind'
        >>> error.details
        'details'

        """
        Exception.__init__(self, message, *args, **kwargs)

        values = raw_error.split(":", 1)

        if len(values) == 2:
            self.details = values[1].strip()
        else:
            self.details = ""

        self.kind = values[0].strip()
        self.type_ = self.kind.lower().strip().replace(" ", "_") if self.kind else None

    @property
    def message(self):
        return self.args[0] if self.args else ""

    def __repr__(self):
        msg = self.message.strip()

        if self.kind and self.details:
            msg += f"\nReason: {self.kind}, {self.details}"

        return msg

    def __str__(self):
        msg = self.message

        if self.kind and self.details:
            msg += f"\nReason: {self.kind}, {self.details}"

        return msg


class PollTimeout(SWFError):
    pass


class InvalidCredentialsError(SWFError):
    pass


class ResponseError(SWFError):
    pass


class DoesNotExistError(SWFError):
    pass


class AlreadyExistsError(SWFError):
    pass


class InvalidKeywordArgumentError(SWFError):
    pass


class RateLimitExceededError(SWFError):
    pass


def ignore(*args, **kwargs):
    return


REGEX_UNKNOWN_RESOURCE = re.compile(r"^[^ ]+\s+([^ :]+)")
REGEX_NESTED_RESOURCE = re.compile(r"Unknown (?:type|execution):\s*([^=]+)=\[")


def match_equals(regex, string, values):
    """
    Extract a value from a string with a regex and compare it.

    :param regex: to extract the value to check.
    :type  regex: _sre.SRE_Pattern (compiled regex)

    :param string: that contains the value to extract.
    :type  string: str

    :param values: to compare with.
    :type  values: [str]

    """
    if string is None:
        return False

    matched = regex.findall(string)
    if not matched:
        return False

    if isinstance(values, str) and not isinstance(values, Sequence):
        values = (values,)
    return matched[0] in values


def is_swf_response_error(error):
    """
    Return true if *error* is a :class:`SWFResponseError` exception.

    :param error: is the exception to check.
    :type  error: Exception.

    """
    return isinstance(error, boto.swf.exceptions.SWFResponseError)


def is_unknown_resource_raised(error, *args, **kwargs):
    """
    Handler that checks if *error* is an unknown resource fault.

    :param error: is the exception to check.
    :type  error: Exception

    """
    if not isinstance(error, boto.swf.exceptions.SWFResponseError):
        return False

    return getattr(error, "error_code", None) == "UnknownResourceFault"


def is_unknown(resource):
    """
    Return a function that checks if *error* is an unknown *resource* fault.

    """

    @wraps(is_unknown)
    def wrapped(error, *args, **kwargs):
        """
        :param error: is the exception to check.
        :type  error: BotoServerError

        """
        if not is_unknown_resource_raised(error, *args, **kwargs):
            return False
        if getattr(error, "error_code", None) != "UnknownResourceFault":
            raise ValueError(f"cannot extract resource from {error}")

        message = error.body.get("message")
        if match_equals(REGEX_UNKNOWN_RESOURCE, message, ("type", "execution")):
            return match_equals(REGEX_NESTED_RESOURCE, message, resource)
        return match_equals(REGEX_UNKNOWN_RESOURCE, message, resource)

    return wrapped


def always(value):
    """
    Always return *value* whatever arguments it got.

    Examples
    --------

    >>> f = always(1)
    >>> f('a', 'b')
    1

    >>> f = always(lambda: True)
    >>> f('foo')
    True

    """
    import types

    @wraps(always)
    def wrapped(*args, **kwargs):
        if isinstance(value, types.FunctionType):
            return value()
        return value

    return wrapped


def extract_resource(error):
    if getattr(error, "error_code", None) != "UnknownResourceFault":
        raise ValueError(f"cannot extract resource from {error}")

    message = error.body.get("message")
    resource = REGEX_UNKNOWN_RESOURCE.findall(message) if message else None
    return "Resource {} does not exist".format(resource[0] if resource else "unknown")


def raises(exception, when, extract=str):
    """
    :param exception: to raise when the predicate is True.
    :type  exception: type(Exception)

    :param when: predicate to apply.
    :type  when: (error, *args, **kwargs) -> bool

    Examples
    --------

    Let's build a :class:`boto.swf.exceptions.SWFResponseError` for an unknown
    execution:

    FIXME commented-out these doctests for now as they fail on python3
    (returning swf.exceptions.DoesNotExistError and such, not just DoesNotExistError)
    # >>> status = 400
    # >>> reason = 'Bad Request'
    # >>> body_type = 'com.amazonaws.swf.base.model#UnknownResourceFault'
    # >>> body_message = 'Unknown execution: blah'
    # >>> body = {'__type': body_type, 'message': body_message}
    # >>> error_code = 'UnknownResourceFault'
    # >>> from boto.swf.exceptions import SWFResponseError
    # >>> err = SWFResponseError(status, reason, body, error_code)
    # >>> raises(DoesNotExistError,
    # ...        when=is_unknown_resource_raised,
    # ...        extract=extract_resource)(err)
    # Traceback (most recent call last):
    #     ...
    # DoesNotExistError: Resource execution does not exist
    #
    # >>> body = {'__type': body_type}
    # >>> err = SWFResponseError(status, reason, body, error_code)
    # >>> raises(DoesNotExistError,
    # ...        when=is_unknown_resource_raised,
    # ...        extract=extract_resource)(err)
    # Traceback (most recent call last):
    #     ...
    # DoesNotExistError: Resource unknown does not exist
    #
    # Now, we do the same for an unknown domain:
    #
    # >>> body_message = 'Unknown domain'
    # >>> body = {'__type': body_type, 'message': body_message}
    # >>> err = SWFResponseError(status, reason, body, error_code)
    # >>> raises(DoesNotExistError,
    # ...        when=is_unknown_resource_raised,
    # ...        extract=extract_resource)(err)
    # Traceback (most recent call last):
    #     ...
    # DoesNotExistError: Resource domain does not exist
    #
    # If it does not detect an error related to an unknown resource,
    # it raises a :class:`ResponseError`:
    #
    # >>> body_message = 'Other Fault'
    # >>> body = {'__type': body_type, 'message': body_message}
    # >>> err = SWFResponseError(status, reason, body, error_code)
    # >>> err.error_code = 'OtherFault'
    # >>> raises(DoesNotExistError,
    # ...        when=is_unknown_resource_raised,
    # ...        extract=extract_resource)(err)
    # ... # doctest: +IGNORE_EXCEPTION_DETAIL
    # Traceback (most recent call last):
    #     ...
    # SWFResponseError: SWFResponseError: 400 Bad Request
    # {'message': 'Other Fault', '__type': 'com.amazonaws.swf.base.model#UnknownResourceFault'}
    #
    # If it's not a :class:`boto.swf.exceptions.SWFResponseError`, it
    # raises the exception as-is:
    #
    # >>> raises(DoesNotExistError,
    # ...        when=is_unknown_resource_raised,
    # ...        extract=extract_resource)(Exception('boom!'))
    # Traceback (most recent call last):
    #     ...
    # Exception: boom!

    """

    @wraps(raises)
    def raises_closure(error, *args, **kwargs):
        if when(error, *args, **kwargs) is True:
            raise exception(extract(error))
        raise error

    return raises_closure


def catch(exceptions, handle_with=None, log=False):
    """
    Catch *exceptions*, then eventually handle and log them.

    :param exceptions: sequence of exceptions to catch.
    :type  exceptions: Exception | (Exception, )

    :param handle_with: handle the exceptions (if handle_with is not None) or
                        raise them again.
    :type  handle_with: function(err, *args, **kwargs)

    :param log: the exception with default logger.
    :type  log: bool

    Examples:

    >>> def boom():
    ...     raise ValueError('test')
    >>> func = catch(ValueError)(boom)
    >>> func()
    Traceback (most recent call last):
        ...
    ValueError: test
    >>> func = catch(ValueError, handle_with=ignore)(boom)
    >>> func()
    >>> func = catch([ValueError], handle_with=ignore)(boom)
    >>> func()

    """

    def wrap(func):
        @wraps(func)
        def decorated(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exceptions as err:
                if log is True:
                    logger.error(f"call to {func.__name__} raised: {err}")

                if handle_with is None:
                    raise

                return handle_with(err, *args, **kwargs)

        return decorated

    if not isinstance(exceptions, Sequence):
        exceptions = tuple([exceptions])
    elif not isinstance(exceptions, tuple):
        exceptions = tuple(exceptions)

    return wrap


is_not = partial(catch, handle_with=always(False))
is_not.__doc__ = """
    Return ``False`` if it catches an exception among *exceptions*.
"""


def translate(exceptions, to):
    """
    Catches an exception among *exceptions* and raise *to* instead.

    """

    def throw(err, *args, **kwargs):
        raise to(err.message)

    return catch(exceptions, handle_with=throw)
