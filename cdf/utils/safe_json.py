from cdf.compat import json
import json as python_json


def loads(string):
    try:
        return json.loads(string)
    except ValueError:
        return python_json.loads(string)