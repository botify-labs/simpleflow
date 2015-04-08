from __future__ import absolute_import


try:
    from .plyvel import LevelDB
except ImportError:
    from .leveldb import LevelDB

from .exceptions import *
