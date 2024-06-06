from __future__ import annotations


def remove_none(obj):
    # Removes None *values* recursively from a dict/list
    # adapted from https://stackoverflow.com/questions/20558699
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(remove_none(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)((k, remove_none(v)) for k, v in obj.items() if v is not None)
    else:
        return obj
