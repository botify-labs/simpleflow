from __future__ import annotations

import re
from collections.abc import Sequence
from functools import partial, wraps
from typing import Any, Callable

from botocore.exceptions import ClientError


class SWFError(Exception):
    def __init__(self, message: str, raw_error: str = "", *args) -> None:
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
        Exception.__init__(self, message, *args)

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


REGEX_UNKNOWN_RESOURCE = re.compile(r"^Unknown ([^ :,]+)")
REGEX_NESTED_RESOURCE = re.compile(r"Unknown (?:type|execution)[:,]\s*([^ =]+)\s*=")


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


def is_unknown_resource_raised(error, *args, **kwargs):
    """
    Handler that checks if *error* is an unknown resource fault.

    :param error: is the exception to check.
    :type  error: Exception

    """
    if not isinstance(error, ClientError):
        return False

    return extract_error_code(error) == "UnknownResourceFault"


def is_unknown(resource: str | Sequence[str]):
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

        error_code = extract_error_code(error)
        if error_code != "UnknownResourceFault":
            raise ValueError(f"cannot extract resource from {error}")

        message = extract_message(error)
        has_nested_resource = match_equals(REGEX_UNKNOWN_RESOURCE, message, ("type", "execution"))
        if has_nested_resource:
            matching = match_equals(REGEX_NESTED_RESOURCE, message, resource)
        else:
            matching = match_equals(REGEX_UNKNOWN_RESOURCE, message, resource)

        return matching

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


def generate_resource_not_found_message(error):
    error_code = extract_error_code(error)
    if error_code != "UnknownResourceFault":
        raise ValueError(f"cannot extract resource from {error}")

    message = extract_message(error)
    resource = REGEX_UNKNOWN_RESOURCE.findall(message) if message else None
    return f"Resource {resource[0] if resource else 'unknown'} does not exist"


def raises(exception, when, extract: Callable[[Any], str] = str):
    """
    :param exception: to raise when the predicate is True.
    :type  exception: type(Exception)

    :param when: predicate to apply.
    :type  when: (error, *args, **kwargs) -> bool
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
    from simpleflow import logger

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
        raise to(extract_message(err))

    return catch(exceptions, handle_with=throw)


def extract_error_code(error: Exception) -> str | None:
    if hasattr(error, "response"):
        return error.response["Error"]["Code"]
    return None


def extract_message(error: Exception) -> str | None:
    if hasattr(error, "response"):
        return error.response["Error"]["Message"]
    return None
