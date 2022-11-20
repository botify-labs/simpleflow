import sys
from pprint import pformat
from typing import TYPE_CHECKING

from . import base

if TYPE_CHECKING:
    from typing import Any


def put_setting(key: str, value: Any):
    setattr(sys.modules[__name__], key, value)
    _keys.add(key)


def configure(dct: dict) -> None:
    for k, v in dct.items():
        put_setting(k, v)


def print_settings():
    for key in sorted(_keys):
        value = getattr(sys.modules[__name__], key)
        print(f"{key}={pformat(value)}")


# initialize a list of settings names
_keys: set[str] = set()

# look for settings and initialize them
configure(base.load())
