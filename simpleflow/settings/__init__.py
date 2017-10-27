import sys

from future.utils import iteritems

from . import base


def put_setting(key, value):
    setattr(sys.modules[__name__], key, value)


def configure(dct):
    for k, v in iteritems(dct):
        put_setting(k, v)


configure(base.load())
