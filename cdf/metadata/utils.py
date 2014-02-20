from . import URLS_DATA_FORMAT_DEFINITION
from cdf.query.es_mapping_generation import generate_complete_field_lookup

_URLS_DATA_FIELDS = generate_complete_field_lookup(URLS_DATA_FORMAT_DEFINITION)


def field_has_children(field):
    prefix = field + '.'
    return any(i[:len(prefix)] == prefix for i in _URLS_DATA_FIELDS)


def children_from_field(field):
    prefix = field + '.'
    return [i for i in _URLS_DATA_FIELDS if i[:len(prefix)] == prefix]

