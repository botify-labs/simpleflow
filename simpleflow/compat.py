# -*- coding: utf-8 -*-
import sys

PY2 = int(sys.version[0]) == 2
PY26 = PY2 and int(sys.version_info[1]) < 7

if PY2:
    from itertools import imap, izip
    import urllib2 as request  # NOQA
    from urllib import quote as urlquote  # NOQA
    text_type = unicode  # NOQA
    binary_type = str
    string_types = (str, unicode)  # NOQA
    unicode = unicode  # NOQA
    basestring = basestring  # NOQA
    imap = imap
    izip = izip
else:
    from urllib import request  # NOQA
    from urllib.parse import quote as urlquote  # NOQA
    text_type = str
    binary_type = bytes
    string_types = (str,)
    unicode = str
    basestring = (str, bytes)
    imap = map
    izip = zip
