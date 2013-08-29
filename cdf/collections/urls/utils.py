from .constants import QUERY_FIELDS


def field_has_children(field):
    return any(i.startswith('{}.'.format(field)) for i in QUERY_FIELDS)


def children_from_field(field):
    return filter(lambda i: i.startswith('{}.'.format(field)), QUERY_FIELDS)
