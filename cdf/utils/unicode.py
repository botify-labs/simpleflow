def deep_clean(d):
    if isinstance(d, (list, tuple)):
        return [deep_clean(k) for k in d]
    elif isinstance(d, dict):
        new_d = {}
        for k, v in d.iteritems():
            new_d[deep_clean(k)] = deep_clean(v)
        return new_d
    elif isinstance(d, unicode):
        return str(d)
    else:
        return d
