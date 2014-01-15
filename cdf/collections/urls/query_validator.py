import valideer as v
import re

from cdf.collections.urls.es_mapping_generation import generate_list_field_lookup
from query_transformer import (_PREDICATE_FORMATS, _LIST_PREDICATES,
                               _NON_LIST_PREDICATES, _UNIVERSAL_PREDICATES,
                               _DEFAULT_PREDICATE)
from cdf.constants import URLS_DATA_FORMAT_DEFINITION


__ALL__ = ['validate_sorts',
           'validate_predicate_filter',
           'validate_boolean_filter',
           'validate_fields']


# Elements in ES that are a list
_LIST_FIELDS = generate_list_field_lookup(URLS_DATA_FORMAT_DEFINITION)

# All available predicates
_AVAILABLE_PREDICATES = _PREDICATE_FORMATS.keys()

# Helper constants
_DESC = 'desc'
_DESC_REGEXP = re.compile('^desc$')
_ORDER = 'order'
_ORDER_REGEXP = re.compile('^order$')
_BOOL_REGEXP = re.compile('^(and|or|not)$')


# Validators
# Sort
class OrderedSortElem(v.Mapping):
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
class BoolFilter(v.Mapping):
    def validate(self, value, adapt=True):
        super(BoolFilter, self).validate(value)
        if len(value) != 1:
            raise v.ValidationError(
                'Boolean predicate contains multiple mapping')
        bool, list_filter = value.iteritems().next()
        if not _BOOL_REGEXP.match(bool):
            raise v.ValidationError(
                'Boolean filter key is not one of `and, or, not`')
        if not isinstance(list_filter, list):
            raise v.ValidationError('Filter list is not a list')

_BOOL_FILTER = BoolFilter()


# Predicate filter
class PredicateFilter(v.Object):
    # fields that can be validate by builtin validators
    basic_value_element = v.AnyOf(v.String(), v.Integer())
    value_field = v.AnyOf(v.HomogeneousSequence(basic_value_element),
                          basic_value_element)
    field_field = v.String()

    def validate(self, value, adapt=True):
        super(PredicateFilter, self).validate(value)
        # required components
        if 'value' not in value:
            raise v.ValidationError(
                'Predicate filter should contain a value field')
        if 'field' not in value:
            raise v.ValidationError(
                'Predicate filter should contain a value field')

        # optional component
        if 'predicate' not in value:
            predicate = _DEFAULT_PREDICATE
        else:
            predicate = value['predicate']

        if predicate not in _AVAILABLE_PREDICATES:
            raise v.ValidationError('Wrong filter predicate')

        # validate value and field structure
        self.value_field.validate(value['value'])
        self.field_field.validate(value['field'])

        # validate predicate field semantic
        predicate_field = value['field']
        if predicate_field in _LIST_FIELDS:
            if predicate in _NON_LIST_PREDICATES:
                raise v.ValidationError(
                    'Apply non-list predicate on list field')
        else:
            if predicate in _LIST_PREDICATES:
                raise v.ValidationError(
                    'Apply list predicate on non-list field')

_PREDICATE_FILTER = PredicateFilter()

# Fields
_FIELDS = v.HomogeneousSequence(v.String())


def validate_sorts(sorts):
    """Validate a list of sorts options in botify front-end query

    :param sorts: a list of sort object
    :raises ValidationError: if sorts structure is not valid
    """
    _SORTS.validate(sorts)


def validate_predicate_filter(predicate_filter):
    _PREDICATE_FILTER.validate(predicate_filter)


def validate_boolean_filter(boolean_filter):
    _BOOL_FILTER.validate(boolean_filter)


def validate_fields(fields):
    _FIELDS.validate(fields)