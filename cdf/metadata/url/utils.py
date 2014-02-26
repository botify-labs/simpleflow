from .url_metadata import URLS_DATA_FORMAT_DEFINITION
from .es_backend_utils import generate_complete_field_lookup
import cdf.metadata.utils as util

_COMPLETE_FIELDS = generate_complete_field_lookup(
    URLS_DATA_FORMAT_DEFINITION)


def has_child(field):
    return util.has_child(field, _COMPLETE_FIELDS)


def get_children(field):
    return util.get_children(field, _COMPLETE_FIELDS)