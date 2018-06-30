# -*- coding: utf-8 -*-
# TODO: remove this file and use future directly.

# noinspection PyUnresolvedReferences
from future.utils import PY2, PY26, PYPY, text_type, binary_type, string_types  # NOQA


if PY2:
    # noinspection PyUnresolvedReferences
    from itertools import imap, izip

    # noinspection PyUnresolvedReferences
    import urllib2 as request  # NOQA

    # noinspection PyUnresolvedReferences
    from urllib import quote as urlquote  # NOQA

    # noinspection PyUnresolvedReferences,PyUnboundLocalVariable,PyCompatibility
    unicode = unicode  # NOQA
    # noinspection PyUnresolvedReferences,PyUnboundLocalVariable,PyCompatibility
    basestring = basestring  # NOQA
    imap = imap
    izip = izip
else:
    # noinspection PyUnresolvedReferences
    from urllib import request  # NOQA

    # noinspection PyUnresolvedReferences,PyCompatibility
    from urllib.parse import quote as urlquote  # NOQA

    unicode = str
    basestring = (str, bytes)
    imap = map
    izip = zip
