# -*- coding: utf-8 -*-
import sys

# noinspection PyUnresolvedReferences
from future.utils import text_type, binary_type, string_types  # NOQA
# noinspection PyUnresolvedReferences,PyCompatibility
from past.types import unicode, basestring  # NOQA


PY3 = sys.version_info[0] == 3
PY2 = sys.version_info[0] == 2
PY26 = sys.version_info[0:2] == (2, 6)
PY27 = sys.version_info[0:2] == (2, 7)
PYPY = hasattr(sys, 'pypy_translation_info')

if PY2:
    # noinspection PyUnresolvedReferences
    from itertools import imap, izip  # NOQA
    # noinspection PyUnresolvedReferences
    import urllib2 as request  # NOQA
    # noinspection PyUnresolvedReferences
    from urllib import quote as urlquote  # NOQA
else:
    # noinspection PyUnresolvedReferences
    from urllib import request  # NOQA
    # noinspection PyUnresolvedReferences,PyCompatibility
    from urllib.parse import quote as urlquote  # NOQA
    imap = map
    izip = zip
