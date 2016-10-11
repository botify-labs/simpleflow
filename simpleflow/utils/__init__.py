from . import retry  # NOQA
from .json_dumps import json_dumps  # NOQA


def issubclass_(arg1, arg2):
    try:
        return issubclass(arg1, arg2)
    except TypeError:
        return False
