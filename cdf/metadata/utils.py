def has_child(field, mapping):
    return any(i.startswith('{}.'.format(field)) for i in mapping)


def get_children(field, mapping):
    return filter(lambda i: i.startswith('{}.'.format(field)), mapping)