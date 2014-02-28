import cdf.metadata.utils as util
from .aggregates_metadata import COUNTERS_FIELDS


def has_child(field):
    return util.has_child(field, COUNTERS_FIELDS)


def get_children(field):
    return util.get_children(field, COUNTERS_FIELDS)