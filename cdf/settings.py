import os

import cdf.utils.discovery


HOST_DISCOVERY = cdf.utils.discovery.UrlHosts

DEFAULT_PYPY_PATH = '/usr/local/virtualenv/pypy/bin/python'
PYPY_PATH = os.environ.get('BOTIFY_CDF_PYPY_PATH', DEFAULT_PYPY_PATH)
ENABLE_PYPY = os.environ.get(
    'BOTIFY_CDF_PYPY_ENABLE', 'true').lower() == 'true'
