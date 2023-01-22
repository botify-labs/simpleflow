from __future__ import annotations

import datetime
import json
import types
from uuid import UUID

import lazy_object_proxy

from simpleflow.futures import Future


def serialize_complex_object(obj):
    if isinstance(obj, bytes):  # Python 3 only (serialize_complex_object not called here in Python 2)
        return obj.decode("utf-8", errors="replace")
    if isinstance(obj, datetime.datetime):
        r = obj.isoformat()
        if obj.microsecond:
            r = r[:23] + r[26:]  # milliseconds only
        if r.endswith("+00:00"):
            r = r[:-6] + "Z"
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
    elif isinstance(obj, lazy_object_proxy.Proxy):
        return obj.__wrapped__
    elif isinstance(obj, (set, frozenset)):
        return list(obj)
    raise TypeError(
        "Type %s couldn't be serialized. This is a bug in simpleflow," " please file a new issue on GitHub!" % type(obj)
    )


def _resolve_proxy(obj):
    if isinstance(obj, dict):
        return {k: _resolve_proxy(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_resolve_proxy(v) for v in obj]
    if isinstance(obj, lazy_object_proxy.Proxy):
        return str(obj)
    return obj


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
        kwargs["default"] = serialize_complex_object
    if pretty:
        kwargs["indent"] = 4
        kwargs["sort_keys"] = True
        kwargs["separators"] = (",", ": ")
    elif compact:
        kwargs["separators"] = (",", ":")
        kwargs["sort_keys"] = True

    try:
        return json.dumps(obj, **kwargs)
    except TypeError:
        # lazy_object_proxy.Proxy subclasses basestring: serialize_complex_object isn't called on python2
        # and some versions of pypy
        obj = _resolve_proxy(obj)
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
    except Exception:
        return data
