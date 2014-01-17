import valideer as v
import re

from cdf.collections.urls.es_mapping_generation import generate_list_field_lookup
from predicate_constants import (PREDICATE_FORMATS, LIST_PREDICATES,
                                 NON_LIST_PREDICATES, UNIVERSAL_PREDICATES,
                                 DEFAULT_PREDICATE, BOOL_PREDICATES, NOT_PREDICATE)
from cdf.constants import URLS_DATA_FORMAT_DEFINITION


__ALL__ = ['validate_sorts',
           'validate_predicate_filter',
           'validate_boolean_filter',
           'validate_fields',
           'validate_not_filter']


# Elements in ES that are a list
_LIST_FIELDS = generate_list_field_lookup(URLS_DATA_FORMAT_DEFINITION)

# All available predicates
_AVAILABLE_PREDICATES = PREDICATE_FORMATS.keys()

# Helper constants
_DESC = 'desc'
_ORDER = 'order'
_DESC_REGEXP = re.compile('^desc$')
_ORDER_REGEXP = re.compile('^order$')
_BOOL_REGEXP = re.compile('^('+'|'.join(BOOL_PREDICATES)+')$')


# Validators

# Sorts
# Verifies that the `sorts` component of a botify front-end query is
# in the form of:
#
#   simple_sort_elem = field
#   ordered_sort_elem = {field: {"order": "desc"}}
#   sorts = [simple_sort_elem | ordered_sort_elem]

class OrderedSortElem(v.Mapping):
    """Valideer validator for an ordered sort element

    Need a custom class for this instead of `valideer.Mapping` b/c
    we need to assert that only a single mapping is contained
    """
    order_object = v.Mapping(v.Pattern('^order$'),
                             v.Pattern('^desc$'))

    def validate(self, value, adapt=True):
        super(OrderedSortElem, self).validate(value)
        if len(value) != 1:
            raise v.ValidationError(
                'Ordered sort element contains multiple mapping')

        field, order = value.iteritems().next()
        self.order_object.validate(order)

_SIMPLE_SORT_ELEM = v.String()
_ORDERED_SORT_ELEM = OrderedSortElem()
_SORTS = v.HomogeneousSequence(v.AnyOf(_ORDERED_SORT_ELEM,
                                       _SIMPLE_SORT_ELEM))


# Boolean filter
# Verifies that a `boolean filter` component of a botify front-end query
# is in the form of:
#
#   boolean_predicate = "and" | "or"
#   boolean_filter = {boolean_predicate : [...]}

class BoolFilter(v.Mapping):
    def validate(self, value, adapt=True):
        super(BoolFilter, self).validate(value)
        if len(value) != 1:
            raise v.ValidationError(
                'Boolean filter contains multiple mapping')
        bool, list_filter = value.iteritems().next()
        if not _BOOL_REGEXP.match(bool):
            raise v.ValidationError(
                'Boolean filter key is not one of `and, or`')
        if not isinstance(list_filter, list):
            raise v.ValidationError('Filter list is not a list')

_BOOL_FILTER = BoolFilter()


# Not filter
# Verifies that a `not filter` component of a botify front-end query
# is in the form of:
#
#   not_query = {"not": filter}

class NotFilter(v.Mapping):
    def validate(self, value, adapt=True):
        super(NotFilter, self).validate(value)
        if len(value) != 1:
            raise v.ValidationError(
                'Not filter contains multiple mapping')
        _not, filter = value.items()[0]
        if _not != NOT_PREDICATE:
            raise v.ValidationError(
                'Not filter key is not `not`')
        if not isinstance(filter, dict):
            raise v.ValidationError('Filter is not a dict')

_NOT_FILTER = NotFilter()


# Predicate filter
# Verifies that a `predicate filter` component of a botify front-end query
# is in the form of:
#
#   basic_value = number | string
#   value = basic_value | [basic_value]
#   predicate_filter = {
#        "predicate": predicate,
#        "field": field,
#        "value": value
#   }
#
# It also verifies query's semantics:
#   - predicate should be a defined operator
#   - restrict list operator to be applied only on list fields, same for
#       non-list operator and non-list fields

# TODO distinguish operator's operand number (values)

class PredicateFilter(v.Object):
    basic_value_element = v.AnyOf(v.String(), v.Integer())
    value_field = v.AnyOf(v.HomogeneousSequence(basic_value_element),
                          basic_value_element)
    field_field = v.String()
    predicate_filter = v.Object(
        required={
            'field': field_field,
        },
        optional={
            'predicate': v.String(),
            'value': value_field
        }
    )

    def validate(self, value, adapt=True):
        super(PredicateFilter, self).validate(value)
        # structural validation
        self.predicate_filter.validate(value)

        # semantic validation
        if 'predicate' not in value:
            predicate = DEFAULT_PREDICATE
        else:
            predicate = value['predicate']

        # TODO need to be separate from structural validation
        if predicate not in _AVAILABLE_PREDICATES:
            raise v.ValidationError('Wrong filter predicate')

        # validate predicate field semantic
        predicate_field = value['field']
        if predicate_field in _LIST_FIELDS:
            if predicate in NON_LIST_PREDICATES:
                raise v.ValidationError(
                    'Apply non-list predicate on list field')
        else:
            if predicate in LIST_PREDICATES:
                raise v.ValidationError(
                    'Apply list predicate on non-list field')

_PREDICATE_FILTER = PredicateFilter()

# Fields
# Verifies that a `fields` component of a botify front-end query
# is in the form of:
#
#   fields = [field]

_FIELDS = v.HomogeneousSequence(v.String())


def validate_sorts(sorts):
    """Validate a list of sorts options in botify front-end query

    :param sorts: a list of sort object
    :raises ValidationError: if sorts structure is not valid
    """
    _SORTS.validate(sorts)


def validate_predicate_filter(predicate_filter):
    """Validate a predicate filter in botify front-end query

    :param predicate_filter: a dict
    :raises ValidationError: if predicate filter structure is not valid
    """
    _PREDICATE_FILTER.validate(predicate_filter)


def validate_boolean_filter(boolean_filter):
    """Validate a boolean filter in botify front-end query

    :param boolean_filter: a dict
    :raises ValidationError: if boolean filter structure is not valid
    """
    _BOOL_FILTER.validate(boolean_filter)


def validate_not_filter(not_filter):
    """Validate a not filter in botify front-end query

    :param not_filter: a dict
    :raises ValidationError: if not filter structure is not valid
    """
    _NOT_FILTER.validate(not_filter)


def validate_fields(fields):
    """Validate the required fileds list in botify front-end query

    :param boolean_filter: a list of required fields
    :raises ValidationError: if the structure is not valid
    """
    _FIELDS.validate(fields)