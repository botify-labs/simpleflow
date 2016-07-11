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


def json_dumps(obj):
    return json.dumps(obj, default=_serialize_complex_object)
