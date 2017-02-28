from uuid import UUID

import datetime
import json
import types

from simpleflow.futures import Future


def _serialize_complex_object(obj):
    if isinstance(obj, datetime.datetime):
        r = obj.isoformat()
        if obj.microsecond:
            r = r[:23] + r[26:]  # milliseconds only
        if r.endswith('+00:00'):
            r = r[:-6] + 'Z'
        return r
    elif isinstance(obj, datetime.date):
        return obj.isoformat()
    elif isinstance(obj, datetime.time):
        r = obj.isoformat()
        if obj.microsecond:
            r = r[:12]
        return r
    elif isinstance(obj, types.GeneratorType):
        return [i for i in obj]
    elif isinstance(obj, Future):
        return obj.result
    elif isinstance(obj, UUID):
        return str(obj)
    raise TypeError(
        "Type %s couldn't be serialized. This is a bug in simpleflow,"
        " please file a new issue on GitHub!" % type(obj))


def json_dumps(obj, pretty=False, compact=True, **kwargs):
    """
    JSON dump to string.
    :param obj:
    :type obj: Any
    :param pretty:
    :type pretty: bool
    :param compact:
    :type compact: bool
    :return:
    :rtype: str
    """
    if "default" not in kwargs:
        kwargs["default"] = _serialize_complex_object
    if pretty:
        kwargs["indent"] = 4
        kwargs["sort_keys"] = True
        kwargs["separators"] = (",", ": ")
    elif compact:
        kwargs["separators"] = (",", ":")
    return json.dumps(obj, **kwargs)


def json_loads_or_raw(data):
    """
    Try to get a JSON object from a string.
    If this isn't JSON, return the raw string.
    :param data: string; should be in JSON format
    :return: JSON-decoded object or raw data
    """
    if not data:
        return None
    try:
        return json.loads(data)
    except ValueError:
        return data
