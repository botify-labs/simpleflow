""" Botify query parsing

Query format definition see:
https://github.com/sem-io/botify-cdf/wiki/Url-Data-Query-Definition

This module parses the input query structure (a python dictionary) into
a intermediate representation which is composed of a hierarchy of internal
classes.
The intermediate representation works like abstract syntax tree
for language parsing, providing methods for transforming to any backend
query API (such as ElasticSearch) and for semantic validation.
"""

import abc
from cdf.collections.urls.es_mapping_generation import (generate_multi_field_lookup,
                                                        generate_list_field_lookup,
                                                        generate_valid_field_lookup)
from cdf.constants import URLS_DATA_FORMAT_DEFINITION, URLS_DATA_FIELDS as CHILD_FIELDS
from cdf.exceptions import BotifyQueryException

__ALL__ = ['parse_botify_query']

# Elements that are of `multi_field` type
_MULTI_FIELDS = generate_multi_field_lookup(URLS_DATA_FORMAT_DEFINITION)

# Elements in ES that are a list
_LIST_FIELDS = generate_list_field_lookup(URLS_DATA_FORMAT_DEFINITION)

# All valid field for querying
_VALID_FIELDS = generate_valid_field_lookup(URLS_DATA_FORMAT_DEFINITION)

# Available sort options
_SORT_OPTIONS = ['desc']

# Boolean filter predicates
_BOOL_PREDICATES = ['and', 'or']

# Not filter predicate
_NOT_PREDICATE = 'not'

# Default ES query components
_DEFAULT_PREDICATE = 'eq'
_DEFAULT_SORT = ['id']
_DEFAULT_FIELD = ['url']


def _get_untouched_field(field):
    """Get the untouched field out of a `multi_field` element

    returns the original field if it's not a `multi_field`
    """
    if field in _MULTI_FIELDS:
        return '%s.untouched' % field
    else:
        return field


def _raise_parsing_error(msg, structure):
    """Helper for raising a query parsing exception"""
    excp_msg = '{} : {}'.format(msg, str(structure))
    raise BotifyQueryException(excp_msg)


# Class hierarchy for representing the parsed query
# All component is sub-class of Term
class Term(object):
    """Abstract class for basic component of a botify query
    """
    @abc.abstractmethod
    def transform(self):
        """Transform to a valid ElasticSearch query component"""
        pass

    @abc.abstractmethod
    def validate(self):
        """Semantic validation"""
        pass


# Filter
#
# Filter
#   - NotFilter
#   - BooleanFilter
#   - PredicateFilter
#       - Variants (each predicate is a sub-class of PredicateFilter)
class Filter(Term):
    pass


class NotFilter(Filter):
    """Class represents a `not` filter

    Attributes:
        filter  the contained filter in dict
    """
    def __init__(self, filter):
        self.filter = filter

    def transform(self):
        return {_NOT_PREDICATE: self.filter.transform()}

    def validate(self):
        self.filter.validate()


class BooleanFilter(Filter):
    """Class represents a `boolean` filter

    Attributes:
        boolean_predicate   boolean operator, could be `and` or `or`
        filters             a list of contained filters
    """
    def __init__(self, boolean_predicate, filters):
        self.boolean_predicate = boolean_predicate
        self.filters = filters

    def transform(self):
        return {
            self.boolean_predicate: [
                filter.transform()
                for filter in self.filters
            ]
        }

    def validate(self):
        for filter in self.filters:
            filter.validate()


# Predicate filter variants
class PredicateFilter(Filter):
    """Base class for all predicate filters

    Attributes:
        predicate_field     target field of this predicate, in string
        value               value needed for this predicate, could be
            a single value, a list or None
    """
    def __init__(self, predicate_field, value=None):
        self.predicate_field = predicate_field
        self.field_value = self.predicate_field.transform()
        self.value = value

    @abc.abstractmethod
    def is_list_op(self):
        """Does this operator works only on list fields?

        :returns: True if it's a list-field specific operator,
            False otherwise. None if it works for both kind of
            fields
        """
        pass

    @abc.abstractmethod
    def nb_operands(self):
        """Get the required number of operands

        :returns: expected number of operands for this predicate
        """
        pass

    def validate(self):
        self.predicate_field.validate()
        self.validate_list_field()
        self.validate_operands()

    def validate_list_field(self):
        """Check that list operators are applied on list fields,
        and vice-versa

        :raises BotifyQueryException: if operator and field are mismatched
        """
        is_list_op = self.is_list_op()
        if is_list_op is None:
            return

        if self.field_value in _LIST_FIELDS and not is_list_op:
            _raise_parsing_error('Apply list predicate on non-list field',
                                 self.field_value)
        elif self.field_value not in _LIST_FIELDS and is_list_op:
            _raise_parsing_error('Apply non-list predicate on list field',
                                 self.field_value)

    def validate_operands(self):
        """Check operands requirement

        :raises BotifyQueryException: if operands numbers mismatch
        """
        nb_operands = self.nb_operands()
        if nb_operands == 0 and self.value is not None:
            _raise_parsing_error('0 operand required, found', self.value)
        elif nb_operands == 1 and (self.value is None or
                                       isinstance(self.value, list)):
            _raise_parsing_error('1 operand required, found', self.value)
        elif nb_operands > 1:
            if not isinstance(self.value, list) or len(self.value) != nb_operands:
                _raise_parsing_error('Multiple operands required, found',
                                     self.value)


class AnyEq(PredicateFilter):
    def is_list_op(self):
        return True

    def nb_operands(self):
        return 1

    def transform(self):
        return {
            'term': {
                self.field_value: self.value
            }
        }


class AnyStarts(PredicateFilter):
    def is_list_op(self):
        return True

    def nb_operands(self):
        return 1

    def transform(self):
        return {
            'prefix': {
                _get_untouched_field(self.field_value): self.value
            }
        }


class AnyEnds(PredicateFilter):
    def is_list_op(self):
        return True

    def nb_operands(self):
        return 1

    def transform(self):
        return {
            'regexp': {
                _get_untouched_field(self.field_value): "@%s" % self.value
            }
        }


class AnyContains(PredicateFilter):
    def is_list_op(self):
        return True

    def nb_operands(self):
        return 1

    def transform(self):
        return {
            'regexp': {
                _get_untouched_field(self.field_value): "@%s@" % self.value
            }
        }


class Contains(PredicateFilter):
    def is_list_op(self):
        return False

    def nb_operands(self):
        return 1

    def transform(self):
        return {
            'regexp': {
                _get_untouched_field(self.field_value): "@%s@" % self.value
            }
        }


class Starts(PredicateFilter):
    def is_list_op(self):
        return False

    def nb_operands(self):
        return 1

    def transform(self):
        return {
            'prefix': {
                _get_untouched_field(self.field_value): self.value
            }
        }


class Ends(PredicateFilter):
    def is_list_op(self):
        return False

    def nb_operands(self):
        return 1

    def transform(self):
        return {
            'regexp': {
                _get_untouched_field(self.field_value): "@%s" % self.value
            }
        }


class Eq(PredicateFilter):
    def is_list_op(self):
        return False

    def nb_operands(self):
        return 1

    def transform(self):
        return {
            'term': {
                self.field_value: self.value
            }
        }


class Re(PredicateFilter):
    def is_list_op(self):
        return False

    def nb_operands(self):
        return 1

    def transform(self):
        return {
            'regexp': {
                self.field_value: self.value
            }
        }


class Gt(PredicateFilter):
    def is_list_op(self):
        return False

    def nb_operands(self):
        return 1

    def transform(self):
        return {
            'range': {
                self.field_value: {
                    'gt': self.value
                }
            }
        }


class Gte(PredicateFilter):
    def is_list_op(self):
        return False

    def nb_operands(self):
        return 1

    def transform(self):
        return {
            'range': {
                self.field_value: {
                    'gte': self.value
                }
            }
        }


class Lt(PredicateFilter):
    def is_list_op(self):
        return False

    def nb_operands(self):
        return 1

    def transform(self):
        return {
            'range': {
                self.field_value: {
                    'lt': self.value
                }
            }
        }


class Lte(PredicateFilter):
    def is_list_op(self):
        return False

    def nb_operands(self):
        return 1

    def transform(self):
        return {
            'range': {
                self.field_value: {
                    'lte': self.value
                }
            }
        }


class Between(PredicateFilter):
    def is_list_op(self):
        return False

    def nb_operands(self):
        return 2

    def transform(self):
        return {
            "range": {
                self.field_value: {
                    "gte": self.value[0],
                    "lte": self.value[1],
                }
            }
        }


class NotNull(PredicateFilter):
    def is_list_op(self):
        return None

    def nb_operands(self):
        return 0

    def transform(self):
        return {
            'exists': {
                'field': self.field_value
            }
        }


_PREDICATE_LIST = {
    # list operators
    'any.eq': AnyEq,
    'any.contains': AnyContains,
    'any.starts': AnyStarts,
    'any.ends': AnyEnds,

    # non-list operators
    'eq': Eq,
    'contains': Contains,
    'starts': Starts,
    'ends': Ends,
    're': Re,
    'lt': Lt,
    'lte': Lte,
    'gt': Gt,
    'gte': Gte,
    'between': Between,

    # universal operators
    'not_null': NotNull
}


def parse_predicate_filter(predicate_filter):
    """Parse a predicate filter structure"""
    if not isinstance(predicate_filter, dict):
        _raise_parsing_error('Predicate filter is not a dict',
                             predicate_filter)
    if not 'field' in predicate_filter:
        _raise_parsing_error('Field is missing in predicate filter',
                             predicate_filter)

    predicate = predicate_filter.get('predicate', _DEFAULT_PREDICATE)
    if predicate not in _PREDICATE_LIST:
        _raise_parsing_error('Unknown predicate', predicate)
    else:
        return _PREDICATE_LIST[predicate](
            PredicateField(predicate_filter['field']),
            predicate_filter['value']
            if 'value' in predicate_filter else None)


def parse_not_filter(not_filter):
    """Parse a not filter structure"""
    if not isinstance(not_filter, dict):
        _raise_parsing_error('Not filter is not a dict',
                             not_filter)
    if len(not_filter) != 1:
        _raise_parsing_error('Not filter has multiple mapping',
                             not_filter)

    _, filter = not_filter.items()[0]
    return NotFilter(parse_filter(filter))


def parse_boolean_filter(boolean_filter):
    """Parse a boolean filter structure"""
    if not isinstance(boolean_filter, dict):
        _raise_parsing_error('Boolean filter is not a dict',
                             boolean_filter)

    if len(boolean_filter) != 1:
        _raise_parsing_error('Boolean filter has multiple mapping',
                             boolean_filter)

    bool_predicate, filters = boolean_filter.items()[0]
    if not isinstance(filters, list):
        _raise_parsing_error('Boolean filter does not contain a list',
                             boolean_filter)
    return BooleanFilter(bool_predicate,
                         [parse_filter(filter) for filter in filters])


def _is_boolean_filter(filter_dict):
    return isinstance(filter_dict, dict) and \
           len(filter_dict) == 1 and \
           filter_dict.keys()[0].lower() in _BOOL_PREDICATES


def _is_not_filter(filter_dict):
    return isinstance(filter_dict, dict) and \
           len(filter_dict) == 1 and \
           filter_dict.keys()[0].lower() == _NOT_PREDICATE


def parse_filter(filter):
    """Parse the filter sub-structure of a botify query"""
    # boolean filter
    if _is_boolean_filter(filter):
        return parse_boolean_filter(filter)
    elif _is_not_filter(filter):
        return parse_not_filter(filter)
    # predicate filter
    else:
        return parse_predicate_filter(filter)


# Sorts
class Sorts(Term):
    """Class represents the sort component of a botify query"""
    def __init__(self, sort_elems):
        self.sort_elems = sort_elems

    def transform(self):
        return [elem.transform()
                for elem in self.sort_elems]

    def validate(self):
        for elem in self.sort_elems:
            elem.validate()


class SortElem(Term):
    pass


class SimpleSortElem(SortElem):
    def __init__(self, sort_field):
        self.sort_field = sort_field
        self.field_value = sort_field.transform()

    def transform(self):
        return {
            self.field_value: {
                'ignore_unmapped': True
            }
        }

    def validate(self):
        self.sort_field.validate()


class OrderedSortElem(SortElem):
    def __init__(self, sort_field, sort_option):
        self.sort_field = sort_field
        self.field_value = sort_field.transform()
        self.sort_option = sort_option

    def transform(self):
        return {
            self.field_value: {
                'ignore_unmapped': True,
                'order': self.sort_option
            }
        }

    def validate(self):
        self.sort_field.validate()
        if self.sort_option not in _SORT_OPTIONS:
            _raise_parsing_error('Unknown sort option',
                                 self.sort_option)


def parse_sort_elem(sort_elem):
    """Parse a single sort option element"""
    if isinstance(sort_elem, str):
        return SimpleSortElem(SortField(sort_elem))
    elif isinstance(sort_elem, dict):
        if len(sort_elem) != 1:
            _raise_parsing_error('Ordered sort element has multiple mapping',
                                 sort_elem)

        sort_field, sort_option = sort_elem.items()[0]
        if not (isinstance(sort_option, dict) and 'order' in sort_option):
            _raise_parsing_error('Sort option has wrong format',
                                 sort_option)
        return OrderedSortElem(SortField(sort_field),
                               sort_option['order'])
    else:
        _raise_parsing_error('Sort element has wrong format',
                             sort_elem)


def parse_sorts(sorts):
    """Parse a sort sub-structure of a botify query"""
    if not isinstance(sorts, list):
        _raise_parsing_error('Sorts is not a list',
                             sorts)

    return Sorts([parse_sort_elem(elem) for elem in sorts])


# Fields
class Fields(Term):
    """Class represents the required fields component of a botify query"""
    def __init__(self, fields):
        self.fields = fields

    def transform(self):
        return [field.transform()
                for field in self.fields]

    def validate(self):
        for field in self.fields:
            field.validate()


class Field(Term):
    def __init__(self, field):
        self.field_value = field

    def transform(self):
        return self.field_value


class RequiredField(Field):
    def validate(self):
        if self.field_value not in _VALID_FIELDS:
            _raise_parsing_error('Field is not valid for query',
                                 self.field_value)


class PredicateField(Field):
    def validate(self):
        if self.field_value not in CHILD_FIELDS:
            _raise_parsing_error("Field is not valid for predicate",
                                 self.field_value)


# TODO allow only number fields ???
class SortField(Field):
    def validate(self):
        if self.field_value not in CHILD_FIELDS:
            _raise_parsing_error("Field is not valid for sort",
                                 self.field_value)


def parse_field(field):
    """Parse a single field"""
    if not isinstance(field, str):
        _raise_parsing_error('Field is not a string',
                             field)
    return RequiredField(field)


def parse_fields(fields):
    """Parse the fields sub-structure of a botify query"""
    if not isinstance(fields, list):
        _raise_parsing_error('Fields is not a list',
                             fields)

    return Fields([parse_field(field)
                   for field in fields])


class BotifyQuery(Term):
    """Class represents the whole front-end query"""
    def __init__(self, fields, sorts, filter):
        self.fields = fields
        self.sorts = sorts
        self.filter = filter

    def transform(self):
        return {
            'query': {'constant_score': {'filter': self.filter.transform()}},
            'sort': self.sorts.transform(),
            'fields': self.fields.transform()
        }

    def validate(self):
        self.fields.validate()
        self.sorts.validate()
        self.filter.validate()


def parse_botify_query(botify_query):
    """Parse a botify front-end query into the intermediate form

    :param botify_query: a dict representing botify front-end query
    :returns: an BotifyQuery object containing the hierarchy of this query
    """
    if not isinstance(botify_query, dict):
        _raise_parsing_error('Botify query is not a dict',
                             botify_query)

    if 'filters' not in botify_query:
        _raise_parsing_error('Filter is missing in botify query',
                             botify_query)

    if 'sort' not in botify_query:
        botify_query['sort'] = _DEFAULT_SORT
    if 'fields' not in botify_query:
        botify_query['fields'] = _DEFAULT_FIELD

    return BotifyQuery(parse_fields(botify_query['fields']),
                       parse_sorts(botify_query['sort']),
                       parse_filter(botify_query['filters']))