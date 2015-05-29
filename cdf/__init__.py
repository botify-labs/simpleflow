#!/usr/bin/env python

# -*- coding: utf-8 -*-

version = (0, 14, 5)
__title__ = "botify-cdf"
__author__ = "ampelmann"
__license__ = "MIT"
__version__ = '.'.join(map(str, version))


# FIXME: Compability workaround to prevent conflicts with six.
from six.moves.urllib.parse import urlparse
