import re
from typing import TYPE_CHECKING
from zlib import adler32

from simpleflow.compat import PY2

from . import retry  # NOQA
from .json_tools import json_dumps, json_loads_or_raw, serialize_complex_object  # NOQA

if TYPE_CHECKING:
    from typing import Any, AnyStr, Type, Union


def issubclass_(arg1, arg2):
    # type: (Union[Type, Any], Type) -> bool
    """
    Like issubclass but without exception.
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
        return "0"
    s = s.encode("utf-8")
    return "{:x}".format(adler32(s) & 0xFFFFFFFF)


def format_exc(exc):
    # type: (Exception) -> AnyStr
    """
    Copy-pasted from traceback._format_final_exc_line.
    :param exc: Exception value
    """
    etype = exc.__class__.__name__
    valuestr = _some_str(exc)
    if exc is None or not valuestr:
        line = "%s" % etype
    else:
        line = "%s: %s" % (etype, valuestr)
    return line


def _some_str(value):
    # type: (Any) -> AnyStr
    """
    Copy-pasted from traceback.
    """
    try:
        return str(value)
    except Exception:
        return "<unprintable %s object>" % type(value).__name__


def format_exc_type(exc_type):
    # type: (Type) -> AnyStr
    type_str = exc_type.__name__
    type_mod = exc_type.__module__
    if type_mod not in ("__main__", "__builtin__", "exceptions", "builtins"):
        type_str = "%s.%s" % (type_mod, type_str)
    return type_str


def to_k8s_identifier(string):
    # NB: K8S identifiers are only lc letters + "." + "-"
    # and we use "." as a separator in many names
    string = string.lower()
    string = re.sub(r"[^a-z-]", "-", string)
    string = re.sub(r"--+", "-", string)
    return string


def import_from_module(path):
    # type: (AnyStr) -> Any
    """
    Import a class or other object: either module.Foo or (builtin) Foo.
    :param path: object name
    :return: object
    :raise ImportError: module not found
    """
    module_path, _, obj_name = path.rpartition(".")
    return import_object_from_module(module_path, obj_name)


def import_object_from_module(module_name, *object_names):
    # type: (AnyStr, *AnyStr) -> Any
    if not module_name:
        module_name = "builtins" if not PY2 else "__builtin__"
    from importlib import import_module

    obj = import_module(module_name)
    for object_name in object_names:
        obj = getattr(obj, object_name)
    return obj


def full_object_name(obj):
    # type: (Any) -> AnyStr
    # Adapted from https://stackoverflow.com/questions/2020014/get-fully-qualified-class-name-of-an-object-in-python
    if isinstance(obj, type):
        return full_class_name(obj)
    return full_class_name(obj.__class__)


def full_class_name(klass):
    # type: (Type) -> AnyStr
    module = klass.__module__
    name = klass.__name__ if PY2 else klass.__qualname__
    if module is None:
        return name
    return module + "." + name
