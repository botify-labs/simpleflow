# -*- coding: utf-8 -*-
import sys

PY2 = int(sys.version[0]) == 2
PY26 = PY2 and int(sys.version_info[1]) < 7

if PY2:
    from itertools import imap
    import urllib2 as request  # NOQA
    from urllib import quote as urlquote  # NOQA
    text_type = unicode
    binary_type = str
    string_types = (str, unicode)
    unicode = unicode
    basestring = basestring
    imap = imap
else:
    from urllib import request  # NOQA
    from urllib.parse import quote as urlquote  # NOQA
    text_type = str
    binary_type = bytes
    string_types = (str,)
    unicode = str
    basestring = (str, bytes)
    imap = map
