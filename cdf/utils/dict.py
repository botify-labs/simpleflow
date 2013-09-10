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


def flatten_dict(init, lkey=''):
    ret = {}
    for rkey, val in init.items():
        key = lkey + rkey
        if isinstance(val, dict):
            ret.update(flatten_dict(val, key + '.'))
        else:
            ret[key] = val
    return ret


def deep_dict(d, split_key='.'):
    """
    Transform a flat dict {"a.b": 1, "b.c": 2} to a deep dict: {"a": {"b": 1 }}, "b": {"c": 2}}
    """
    new_d = {}
    for k, v in d.iteritems():
        deep_update(new_d, reduce(lambda x, y: {y: x}, reversed(k.split('.') + [v])))
    return new_d
