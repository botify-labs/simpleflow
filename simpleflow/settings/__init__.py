import sys

from future.utils import iteritems

from . import base


def put_setting(key, value):
    setattr(sys.modules[__name__], key, value)
    _keys.add(key)


def configure(dct):
    for k, v in iteritems(dct):
        put_setting(k, v)


# initialize a list of settings names
_keys = set()

# look for settings and initialize them
configure(base.load())
