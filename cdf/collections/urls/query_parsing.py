import abc
from cdf.collections.urls.es_mapping_generation import (generate_multi_field_lookup,
                                                        generate_list_field_lookup)
from cdf.constants import URLS_DATA_FORMAT_DEFINITION
from cdf.exceptions import BotifyQueryException


# Elements that are of `multi_field` type
_MULTI_FIELDS = generate_multi_field_lookup(URLS_DATA_FORMAT_DEFINITION)

# Elements in ES that are a list
_LIST_FIELDS = generate_list_field_lookup(URLS_DATA_FORMAT_DEFINITION)


_SORT_OPTIONS = ['desc']


# Boolean filter predicates
_BOOL_PREDICATES = ['and', 'or']


# Not filter predicate
_NOT_PREDICATE = 'not'


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
    excp_msg = '{} : {}'.format(msg, str(structure))
    raise BotifyQueryException(excp_msg)


def _check_list_field(field, is_list_operator=True):
    if field in _LIST_FIELDS and not is_list_operator:
        _raise_parsing_error('Apply list predicate on non-list field',
                             field)
    elif field not in _LIST_FIELDS and is_list_operator:
        _raise_parsing_error('Apply non-list predicate on list field',
                             field)


# TODO replace this with operand parsing
def _check_operands(value, required_nb=1):
    if required_nb == 0 and value != None:
        _raise_parsing_error('0 operand required, found',
                             value)
    elif required_nb == 1 and value == None and not isinstance(value, list):
        _raise_parsing_error('1 operand required, found',
                             value)
    elif required_nb > 1:
        if not isinstance(value, list) or len(value) != required_nb:
            _raise_parsing_error('Multiple operands required, found',
                                 value)


class Term(object):
    """Abstract class for basic component of a botify query
    """
    @abc.abstractmethod
    def transform(self):
        """Transform the predicate structure"""
        pass

    @abc.abstractmethod
    def validate(self):
        """Semantic validation"""
        pass


# Filter
class Filter(Term):
    pass


class NotFilter(Filter):
    def __init__(self, filter):
        self.filter = filter

    def transform(self):
        return {_NOT_PREDICATE: self.filter.transform()}


class BooleanFilter(Filter):
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


# Predicate filter variants
class PredicateFilter(Filter):
    def __init__(self, field, value=None):
        self.field = field
        self.value = value
        self.validate()


class AnyEq(PredicateFilter):
    def transform(self):
        return {
            'term': {
                self.field: self.value
            }
        }

    def validate(self):
        _check_list_field(self.field, is_list_operator=True)
        _check_operands(self.value, 1)


class AnyStarts(PredicateFilter):
    def transform(self):
        return {
            'prefix': {
                _get_untouched_field(self.field): self.value
            }
        }

    def validate(self):
        _check_list_field(self.field, is_list_operator=True)
        _check_operands(self.value, 1)


class AnyEnds(PredicateFilter):
    def transform(self):
        return {
            'regexp': {
                _get_untouched_field(self.field): "@%s" % self.value
            }
        }

    def validate(self):
        _check_list_field(self.field, is_list_operator=True)
        _check_operands(self.value, 1)


class AnyContains(PredicateFilter):
    def transform(self):
        return {
            'regexp': {
                _get_untouched_field(self.field): "@%s@" % self.value
            }
        }

    def validate(self):
        _check_list_field(self.field, is_list_operator=True)
        _check_operands(self.value, 1)


class Contains(PredicateFilter):
    def transform(self):
        return {
            'regexp': {
                _get_untouched_field(self.field): "@%s@" % self.value
            }
        }

    def validate(self):
        _check_list_field(self.field, is_list_operator=False)
        _check_operands(self.value, 1)


class Starts(PredicateFilter):
    def transform(self):
        return {
            'prefix': {
                _get_untouched_field(self.field): self.value
            }
        }

    def validate(self):
        _check_list_field(self.field, is_list_operator=False)
        _check_operands(self.value, 1)


class Ends(PredicateFilter):
    def transform(self):
        return {
            'regexp': {
                _get_untouched_field(self.field): "@%s" % self.value
            }
        }

    def validate(self):
        _check_list_field(self.field, is_list_operator=False)
        _check_operands(self.value, 1)


class Eq(PredicateFilter):
    def transform(self):
        return {
            'term': {
                self.field: self.value
            }
        }

    def validate(self):
        _check_list_field(self.field, is_list_operator=False)
        _check_operands(self.value, 1)


class Re(PredicateFilter):
    def transform(self):
        return {
            'regexp': {
                self.field: self.value
            }
        }

    def validate(self):
        _check_list_field(self.field, is_list_operator=False)
        _check_operands(self.value, 1)


class Gt(PredicateFilter):
    def transform(self):
        return {
            'range': {
                self.field: {
                    'gt': self.value
                }
            }
        }

    def validate(self):
        _check_list_field(self.field, is_list_operator=False)
        _check_operands(self.value, 1)


class Gte(PredicateFilter):
    def transform(self):
        return {
            'range': {
                self.field: {
                    'gte': self.value
                }
            }
        }

    def validate(self):
        _check_list_field(self.field, is_list_operator=False)
        _check_operands(self.value, 1)


class Lt(PredicateFilter):
    def transform(self):
        return {
            'range': {
                self.field: {
                    'lt': self.value
                }
            }
        }

    def validate(self):
        _check_list_field(self.field, is_list_operator=False)
        _check_operands(self.value, 1)


class Lte(PredicateFilter):
    def transform(self):
        return {
            'range': {
                self.field: {
                    'lte': self.value
                }
            }
        }

    def validate(self):
        _check_list_field(self.field, is_list_operator=False)
        _check_operands(self.value, 1)


class Between(PredicateFilter):
    def transform(self):
        return {
            "range": {
                self.field: {
                    "gte": self.value[0],
                    "lte": self.value[1],
                }
            }
        }

    def validate(self):
        _check_list_field(self.field, is_list_operator=False)
        _check_operands(self.value, 2)


class NotNull(PredicateFilter):
    def transform(self):
        return {
           'exists': {
                'field': self.field
            }
        }

    def validate(self):
        _check_operands(self.value, 0)


_PREDICATE_DISPATCH = {
    'any.eq': AnyEq,
    'any.contains': AnyContains,
    'any.starts': AnyStarts,
    'any.ends': AnyEnds,

    'eq': Eq,
    'contains': Contains,
    'starts': Starts,
    'ends': Ends,
    're': Re,
    'lt': Lt,
    'lte': Lte,
    'gt': Gt,
    'gte': Gte,
    'not_null': NotNull,
    'between': Between
}


def parse_predicate_filter(predicate_filter):
    if not isinstance(predicate_filter, dict):
        _raise_parsing_error('Predicate filter is not a dict',
                             predicate_filter)
    if not 'field' in predicate_filter:
        _raise_parsing_error('Field is missing in predicate filter',
                             predicate_filter)

    predicate = predicate_filter.get('predicate', 'eq')
    if predicate not in _PREDICATE_DISPATCH:
        _raise_parsing_error('Unknown predicate', predicate)
    else:
        return _PREDICATE_DISPATCH[predicate](predicate_filter['field'],
                                              predicate_filter['value']
                                              if 'value' in predicate_filter
                                              else None)


def parse_not_filter(not_filter):
    if not isinstance(not_filter, dict):
        _raise_parsing_error('Not filter is not a dict',
                             not_filter)
    if len(not_filter) != 1:
        _raise_parsing_error('Not filter has multiple mapping',
                             not_filter)

    _, filter = not_filter.items()[0]
    return NotFilter(parse_filter(filter))


def parse_boolean_filter(boolean_filter):
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
    def __init__(self, sort_elems):
        self.sort_elems = sort_elems

    def transform(self):
        return [elem.transform()
                for elem in self.sort_elems]


class SortElem(Term):
    pass


class SimpleSortElem(SortElem):
    def __init__(self, sort_field):
        self.sort_field = sort_field

    def transform(self):
        return {
            self.sort_field: {
                'ignore_unmapped': True
            }
        }

    def validate(self):
        pass


class OrderedSortElem(SortElem):
    def __init__(self, sort_field, sort_option):
        self.sort_field = sort_field
        self.sort_option = sort_option
        self.validate()

    def transform(self):
        return {
            self.sort_field: {
                'ignore_unmapped': True,
                'order': self.sort_option
            }
        }

    def validate(self):
        if self.sort_option not in _SORT_OPTIONS:
            _raise_parsing_error('Unknown sort option',
                                 self.sort_option)


def parse_sort_elem(sort_elem):
    if isinstance(sort_elem, str):
        return SimpleSortElem(sort_elem)
    elif isinstance(sort_elem, dict):
        if len(sort_elem) != 1:
            _raise_parsing_error('Ordered sort element has multiple mapping',
                                 sort_elem)

        sort_field, sort_option = sort_elem.items()[0]
        if not (isinstance(sort_option, dict) and 'order' in sort_option):
            _raise_parsing_error('Sort option has wrong format',
                                 sort_option)
        return OrderedSortElem(sort_field, sort_option['order'])
    else:
        _raise_parsing_error('Sort element has wrong format',
                             sort_elem)


def parse_sorts(sorts):
    if not isinstance(sorts, list):
        _raise_parsing_error('Sorts is not a list',
                             sorts)

    return Sorts([parse_sort_elem(elem) for elem in sorts])


# Fields
class Fields(Term):
    def __init__(self, fields):
        self.fields = fields

    def transform(self):
        return [field.transform()
                for field in self.fields]

    def validate(self):
        pass


# TODO validate field
class Field(Term):
    def __init__(self, field):
        self.field = field

    def transform(self):
        return self.field

    def validate(self):
        pass


def parse_field(field):
    if not isinstance(field, str):
        _raise_parsing_error('Field is not a string',
                             field)
    return Field(field)


def parse_fields(fields):
    if not isinstance(fields, list):
        _raise_parsing_error('Fields is not a list',
                             fields)

    return Fields([parse_field(field)
                   for field in fields])


class BotifyQuery(Term):
    def __init__(self, fields, sorts, filter):
        self.fields = fields
        self.sorts = sorts
        self.filter = filter
        self.validate()

    def transform(self):
        return {
            'query': {'constant_score': {'filter': self.filter.transform()}},
            'sort': self.sorts.transform(),
            'fields': self.fields.transform()
        }


def parse_botify_query(botify_query):
    if not isinstance(botify_query, dict):
        _raise_parsing_error('Query is not a dict',
                             botify_query)

    if 'filters' not in botify_query:
        _raise_parsing_error('Filter is missing',
                             botify_query)

    if 'sort' not in botify_query:
        botify_query['sort'] = _DEFAULT_SORT
    if 'fields' not in botify_query:
        botify_query['fields'] = _DEFAULT_FIELD

    return BotifyQuery(parse_fields(botify_query['fields']),
                       parse_sorts(botify_query['sort']),
                       parse_filter(botify_query['filters']))