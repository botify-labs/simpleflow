# compat on json module between pypy and cpython
try:
    import ujson as json
except ImportError:
    import json