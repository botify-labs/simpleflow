import collections


def deep_update(d, u, depth=-1):
    """
    Recursively merge or update dict-like objects.
    >>> deep_update({'k1': {'k2': 2}}, {'k1': {'k2': {'k3': 3}}, 'k4': 4})
    {'k1': {'k2': {'k3': 3}}, 'k4': 4}
    """

    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping) and not depth == 0:
            r = deep_update(d.get(k, {}), v, depth=max(depth - 1, -1))
            d[k] = r
        elif isinstance(d, collections.Mapping):
            d[k] = u[k]
        else:
            d = {k: u[k]}
    return d


def deep_clean(d):
    if not isinstance(d, dict):
        return d

    new_d = {}
    for k, v in d.iteritems():
        if isinstance(v, dict):
            new_d[str(k)] = deep_clean(v)
        elif isinstance(v, unicode):
            new_d[str(k)] = str(v)
        else:
            new_d[str(k)] = v
    return new_d


def flatten_dict(init, lkey=''):
    ret = {}
    for rkey, val in init.items():
        key = lkey + rkey
        if isinstance(val, dict):
            ret.update(flatten_dict(val, key + '.'))
        else:
            ret[key] = val
    return ret
