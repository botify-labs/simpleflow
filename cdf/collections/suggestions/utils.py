from .constants import COUNTERS_FIELDS


def field_has_children(field):
    return any(i.startswith('{}.'.format(field)) for i in COUNTERS_FIELDS)


def children_from_field(field):
    return filter(lambda i: i.startswith('{}.'.format(field)), COUNTERS_FIELDS)
