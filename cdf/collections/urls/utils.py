from .constants import QUERY_FIELDS


def field_has_children(field):
    prefix = field + '.'
    return any(i[:len(prefix)] == prefix for i in QUERY_FIELDS)


def children_from_field(field):
    prefix = field + '.'
    return [i for i in QUERY_FIELDS if i[:len(prefix)] == prefix]
