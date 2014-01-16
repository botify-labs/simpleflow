from copy import deepcopy

from cdf.collections.urls.predicate_constants import (DEFAULT_PREDICATE, PREDICATE_FORMATS,
                                                      BOOL_PREDICATES, NOT_PREDICATE)
from cdf.exceptions import BotifyQueryException


__ALL__ = ['get_es_query']

_DEFAULT_SORT = [{'id': {'ignore_unmapped': True}}]
_DEFAULT_FIELD = ['url']


def _is_boolean_filter(filter_dict):
    return isinstance(filter_dict, dict) and \
           len(filter_dict) == 1 and \
           filter_dict.keys()[0].lower() in BOOL_PREDICATES


def _is_not_filter(filter_dict):
    return isinstance(filter_dict, dict) and \
           len(filter_dict) == 1 and \
           filter_dict.keys()[0].lower() == NOT_PREDICATE


def _parse_predicate_filter(predicate_filter):
    predicate = predicate_filter.get('predicate', DEFAULT_PREDICATE)
    return PREDICATE_FORMATS[predicate](predicate_filter)


def _parse_filter(filter):
    # boolean filter
    if _is_boolean_filter(filter):
        # TODO validate
        operator, filters = filter.items()[0]
        return {operator: [_parse_filter(f) for f in filters]}
    elif _is_not_filter(filter):
        # TODO validate
        _, other_filter = filter.items()[0]
        return {NOT_PREDICATE: _parse_filter(other_filter)}
    # predicate filter
    else:
        # TODO validate
        return _parse_predicate_filter(filter)


def _merge_filters(query, filters):
    """Merge filters to botify query using `and` filter

    New filters are places before the original filters.

    :param query: the botify format query
    :param filters: a list of botify predicate to merge
    :return: the extended query
    """
    botify_query = deepcopy(query)
    to_merge = deepcopy(filters)

    if not 'filters' in botify_query:
        botify_query['filters'] = {'and': to_merge}
        return botify_query

    # try to merge into existing, outer `and` filter
    if 'and' in botify_query['filters']:
        botify_query['filters']['and'] = filters + botify_query['filters']['and']
        return botify_query

    # create a new `and` filter for merging
    to_merge.append(botify_query['filters'])
    botify_query['filters'] = {'and': to_merge}
    return botify_query


def _parse_sorts(sorts):
    """Process sort options and add default sort parameters

    :param sorts: a list of sort options of botify_query format,
        eg. ['url', {'depth': {'order': 'desc'}}]
    :returns: the corresponding ElasticSearch query sort component
    """
    es_sorts = []
    for sort in sorts:
        if isinstance(sort, dict):
            # sort contains order option
            field = list(sort.keys())[0]
            es_sorts.append({
                field: {
                    'ignore_unmapped': True,
                    'order': sort[field]['order']
                }
            })
        elif isinstance(sort, str):
            es_sorts.append({
                sort: {
                    'ignore_unmapped': True,
                }
            })
        else:
            raise BotifyQueryException(
                "Wrong query format in sort: {}".format(sort))
    return es_sorts


def _parse_fields(fields):
    # TODO validate
    return fields


def _wrap_query(unwrapped):
    """Wrap a processed botify query into its final form

    Currently a `constant_score` query is used

    :param unwrapped: processed botify query containing
        `fields`, `filter` and `sort`
    """
    filters = {'filter': unwrapped['filter']}
    return {
        'query': {'constant_score': filters},
        'sort': unwrapped['sort'],
        'fields': unwrapped['fields']
    }


def get_es_query(botify_query, crawl_id):
    # By default all queries should have these filter/predicate
    #   1. only query for current crawl/site
    #   2. only query for urls whose http_code != 0 (crawled urls)
    # The order is important for and/or/not filters in ElasticSearch
    # See: http://www.elasticsearch.org/blog/all-about-elasticsearch-filter-bitsets/
    default_filters = [
        {'field': 'crawl_id', 'value': crawl_id},
        {'not': {'field': 'http_code', 'value': 0, 'predicate': 'eq'}}
    ]

    # Merge default filters in botify format query
    botify_query = _merge_filters(botify_query, default_filters)

    # Transform botify query to ElasticSearch query
    es_query = {}

    if 'sort' in botify_query:
        es_query['sort'] = _parse_sorts(botify_query['sort'])
    else:
        es_query['sort'] = _DEFAULT_SORT

    if 'filters' in botify_query:
        es_query['filter'] = _parse_filter(botify_query['filters'])
    else:
        raise BotifyQueryException(
            'No filter component in query: {}'.format(botify_query))

    if 'fields' in botify_query:
        es_query['fields'] = _parse_fields(botify_query['fields'])
    else:
        es_query['fields'] = _DEFAULT_FIELD

    return _wrap_query(es_query)