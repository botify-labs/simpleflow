from zlib import adler32

from . import retry  # NOQA
from .json_tools import json_dumps, json_loads_or_raw  # NOQA


def issubclass_(arg1, arg2):
    """
    Like issubclass but without exception.
    :param arg1:
    :type arg1: object
    :param arg2:
    :type arg2: type
    :return: True for a subclass
    :rtype: bool
    """
    try:
        return issubclass(arg1, arg2)
    except TypeError:
        return False


def hex_hash(s):
    """
    Hex hash of a string. Not too much constrained
    :param s:
    :return:
    """
    if not s:
        return '0'
    s = s.encode('utf-8')
    return '{:x}'.format(adler32(s) & 0xffffffff)
