import sys

from future.utils import iteritems

from . import base


for k, v in iteritems(base.load()):
    setattr(sys.modules[__name__], k, v)
