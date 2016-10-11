from datetime import datetime
import json
import types


def _serialize_complex_object(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, types.GeneratorType):
        return [i for i in obj]
    raise TypeError(
        "Type %s couldn't be serialized. This is a bug in simpleflow,"
        " please file a new issue on GitHub!" % type(obj))


def json_dumps(obj, pretty=False, compact=True):
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
    kwargs = {
        "default": _serialize_complex_object
    }
    if pretty:
        kwargs["indent"] = 4
        kwargs["sort_keys"] = True
        kwargs["separators"] = (",", ": ")
    elif compact:
        kwargs["separators"] = (",", ":")
    return json.dumps(obj, **kwargs)
