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


def get_subdict_from_path(path, dict):
    """Get the corresponding sub-dict or value from a dict by a
    dot separated path

        For example, _dict = {'a': {'b': {'c': 1}}} called with
        path 'a.b.c' will return the value 1. With path 'a.b' will
        return the sub dict {'c': 1}

    :param path: a valid dot separated path
    :returns: sub dict or value
    """
    return reduce(lambda d, key: d[key], path.split('.'), dict)


def path_in_dict(path, dict):
    """Check whether a path is valid in a dict

    :param path: dot separated path to check
    """
    res = True
    try:
        get_subdict_from_path(path, dict)
    except KeyError:
        res = False
    return res


def update_path_in_dict(path, value, _dict):
    keys = path.split('.')
    _len = len(keys)
    current = _dict
    for i in xrange(0, _len):
        key = keys[i]
        if i is _len - 1:
            # override existing value
            # create non-existing value
            current[key] = value
        else:
            if key in current:
                _next = current[key]
                if isinstance(_next, dict):
                    current = _next
                else:
                    _next = {}
                    current = _next
            else:
                current[key] = {}
                current = current[key]


def update_dict(dict_to_update, update):
    """Merge an update dict in a non-overridden way"""
    for item in flatten_dict(update).iteritems():
        path, value = item
        update_path_in_dict(path, value, dict_to_update)
