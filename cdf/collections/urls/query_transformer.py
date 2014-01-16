from copy import deepcopy
from cdf.collections.urls.predicate_constants import DEFAULT_PREDICATE, PREDICATE_FORMATS

from cdf.exceptions import BotifyQueryException


__ALL__ = ['get_es_query']


def _is_boolean_filter(filter_dict):
    return isinstance(filter_dict, dict) and \
           len(filter_dict) == 1 and \
           filter_dict.keys()[0].lower() in ('and', 'or')


def _process_filters(filters, has_parent=False):
    if _is_boolean_filter(filters):
        operator = filters.keys()[0].lower()
        return {operator: _process_filters(filters.values()[0], True)}
    elif isinstance(filters, list) and not has_parent:
        return {"and": [_process_filters(f, True) for f in filters]}
    elif isinstance(filters, list):
        return [_process_filters(f, True) for f in filters]
    else:
        predicate = filters.get('predicate', DEFAULT_PREDICATE)
        if filters.get('not', False):
            return {"not": PREDICATE_FORMATS[predicate](filters)}
        else:
            return PREDICATE_FORMATS[predicate](filters)


# TODO(darkjh) nested `and` and `or` can be simplified
def _add_filters(query, filters):
    """Append some filters to botify format query using `and` operator

    :param botify_query: the botify format query
    :param filters: a list of botify predicate to merge
    :return: the appended query
    """
    botify_query = deepcopy(query)
    if not 'filters' in botify_query:
        botify_query['filters'] = {'and': filters}
    elif isinstance(botify_query['filters'], dict) and not any(k in ('and', 'or') for
                                                               k in botify_query['filters'].keys()):
        botify_query['filters'] = {'and': filters + [botify_query['filters']]}
    elif 'and' in botify_query['filters']:
        # TODO(darkjh) a dict inside and/or ???
        if isinstance(botify_query['filters']['and'], dict):
            botify_query['filters']['and'] = [botify_query['filters']['and'], filters]
        else:
            botify_query['filters']['and'] = filters + botify_query['filters']['and']
    elif 'or' in botify_query['filters']:
        botify_query['filters']['and'] = [{'and': filters}, {'or': botify_query['filters']['or']}]
        del botify_query['filters']['or']
    else:
        raise Exception('filters are not valid for given es_query')

    return botify_query


def _process_sorts(sorts):
    """Process sort options and add default sort parameters

    :param sorts: a list of sort options of botify_query format,
        eg. ['url', {'depth': {'order': 'desc'}}]
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
        {'field': 'http_code', 'value': 0, 'predicate': 'eq', 'not': True}
    ]

    # Merge default filters in botify format query
    botify_query = _add_filters(botify_query, default_filters)

    # Transform botify query to ElasticSearch query
    es_query = {}

    if 'sort' in botify_query:
        es_query['sort'] = _process_sorts(botify_query['sort'])
    else:
        es_query['sort'] = [{'id': {'ignore_unmapped': True}}]

    if 'filters' in botify_query:
        es_query['filter'] = _process_filters(botify_query['filters'])

    if 'fields' in botify_query:
        es_query['fields'] = botify_query['fields']
    else:
        es_query['fields'] = ['url']

    return _wrap_query(es_query)