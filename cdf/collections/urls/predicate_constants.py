from cdf.collections.urls.es_mapping_generation import generate_multi_field_lookup
from cdf.constants import URLS_DATA_FORMAT_DEFINITION


__ALL__ = ['LIST_PREDICATES',
           'NON_LIST_PREDICATES',
           'UNIVERSAL_PREDICATES',
           'DEFAULT_PREDICATE']


# Elements that are of `multi_field` type
_MULTI_FIELDS = generate_multi_field_lookup(URLS_DATA_FORMAT_DEFINITION)


def _get_untouched_field(field):
    """Get the untouched field out of a `multi_field` element

    returns the original field if it's not a `multi_field`
    """
    if field in _MULTI_FIELDS:
        return '%s.untouched' % field
    else:
        return field


# Predicate by default
DEFAULT_PREDICATE = 'eq'


# Predicates that workds only with list fields
LIST_PREDICATES = {
    'any.eq': lambda filters: {
        "term": {
            filters['field']: filters['value'],
        }
    },
    'any.starts': lambda filters: {
        "prefix": {
            _get_untouched_field(filters['field']): filters['value'],
        }
    },
    'any.ends': lambda filters: {
        "regexp": {
            _get_untouched_field(filters['field']): "@%s" % filters['value']
        }
    },

    'any.contains': lambda filters: {
        "regexp": {
            _get_untouched_field(filters['field']): "@%s@" % filters['value']
        }
    }
}


# Predicates that works only with non-list fields
NON_LIST_PREDICATES = {
    're': lambda filters: {
        "regexp": {
            filters['field']: filters['value']
        }
    },
    'gte': lambda filters: {
        "range": {
            filters['field']: {
                "from": filters['value'],
            }
        }
    },
    'gt': lambda filters: {
        "range": {
            filters['field']: {
                "gt": filters['value']
            }
        }
    },
    'lte': lambda filters: {
        "range": {
            filters['field']: {
                "lte": filters['value'],
            }
        }
    },
    'lt': lambda filters: {
        "range": {
            filters['field']: {
                "lt": filters['value'],
            }
        }
    },
    'contains': lambda filters: {
        "regexp": {
            _get_untouched_field(filters['field']): "@%s@" % filters['value']
        }
    },
    'eq': lambda filters: {
        "term": {
            filters['field']: filters['value'],
        }
    },
    # 'starts' predicate should be applied on `untouched`
    'starts': lambda filters: {
        "prefix": {
            _get_untouched_field(filters['field']): filters['value'],
        }
    },
    # 'ends' predicate should be applied on `untouched`
    'ends': lambda filters: {
        "regexp": {
            _get_untouched_field(filters['field']): "@%s" % filters['value']
        }
    },
    'between': lambda filters: {
        "range": {
            filters['field']: {
                "gte": filters['value'][0],
                "lte": filters['value'][1],
            }
        }
    }
}


# Predicates that works both with list and non-list fields
UNIVERSAL_PREDICATES = {
    'not_null': lambda filters: {
        'exists': {
            'field': filters['field']
        }
    }
}


# All available predicates
PREDICATE_FORMATS = dict(LIST_PREDICATES.items() +
                         NON_LIST_PREDICATES.items() +
                         UNIVERSAL_PREDICATES.items())
