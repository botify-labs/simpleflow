import re
from zlib import adler32

from . import retry  # NOQA
from .json_tools import json_dumps, json_loads_or_raw, serialize_complex_object  # NOQA


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


def format_exc(exc):
    """
    Copy-pasted from traceback._format_final_exc_line.
    :param exc: Exception value
    :type exc: Exception
    :return: String
    :rtype: str
    """
    etype = exc.__class__.__name__
    valuestr = _some_str(exc)
    if exc is None or not valuestr:
        line = "%s" % etype
    else:
        line = "%s: %s" % (etype, valuestr)
    return line


def _some_str(value):
    """
    Copy-pasted from traceback.
    """
    try:
        return str(value)
    except:
        return '<unprintable %s object>' % type(value).__name__


def to_k8s_identifier(string):
    # NB: K8S identifiers are only lc letters + "." + "-"
    # and we use "." as a separator in many names
    string = string.lower()
    string = re.sub(r"[^a-z-]", "-", string)
    string = re.sub(r"--+", "-", string)
    return string
