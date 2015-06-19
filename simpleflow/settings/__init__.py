import sys

import base


for k, v in base.load().iteritems():
    setattr(sys.modules[__name__], k, v)
