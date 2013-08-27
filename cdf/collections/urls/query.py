from pyelasticsearch import ElasticSearch

from cdf.constants import URLS_DATA_MAPPING
from cdf.utils.dict import deep_update, deep_clean
from .constants import QUERY_FIELDS, QUERY_TAGGING_FIELDS


def field_has_children(field):
    return any(i.startswith('{}.'.format(field)) for i in QUERY_FIELDS)


def children_from_field(field):
    return filter(lambda i: i.startswith('{}.'.format(field)), QUERY_FIELDS)


def prepare_redirects_from(query, es_document):
    if 'redirects_from' in es_document:
        for _r in es_document['redirects_from']:
            query._urls_ids.add(_r['url_id'])


def transform_redirects_from(query, es_document, attributes):
    if 'redirects_from' in es_document:
        attributes['redirects_from'] = []
        for _r in es_document['redirects_from']:
            attributes['redirects_from'].append({
                'http_code': _r['http_code'],
                'url': {
                    'url': query._id_to_url.get(_r['url_id'])[0],
                    'crawled': True
                }
            })


def prepare_redirects_to(query, es_document):
    if 'redirects_to' in es_document and 'url_id' in es_document['redirects_to']:
        query._urls_ids.add(es_document['redirects_to']['url_id'])


def transform_redirects_to(query, es_document, attributes):
    if 'redirects_to' in es_document:
        if 'url_id' in es_document['redirects_to']:
            url, http_code = query._id_to_url.get(es_document['redirects_to']['url_id'])
            attributes['redirects_to'] = {
                'url': url,
                'crawled': http_code > 0
            }
        elif 'url' in es_document['redirects_to']:
            """
            It's an external url
            """
            attributes['redirects_to'] = {
                'url': es_document['redirects_to']['url'],
                'crawled': False
            }


def prepare_links(query, es_document, link_direction, link_type):
    if link_direction in es_document and link_type in es_document[link_direction]:
        query._urls_ids |= set(es_document[link_direction][link_type])


def transform_links(query, es_document, attributes, link_direction, link_type):
    if not link_direction in attributes:
        attributes[link_direction] = {}
    if not link_type in attributes[link_direction]:
        attributes[link_direction][link_type] = []
    if not link_direction in es_document or not link_type in es_document[link_direction]:
        return
    for url_id in es_document[link_direction][link_type]:
        url, http_code = query._id_to_url.get(url_id)
        attributes[link_direction][link_type].append(
            {
                'url': str(url),
                'crawled': http_code > 0
            }
        )


"""
def prepare_metadata_duplicate(query, es_document, field):
    if 'metadata_duplicate' in es_document and field in es_document['metadata_duplicate']:
        query._urls_ids |= set(es_document['metadata_duplicate'][field])


def transform_metadata_duplicate(query, es_document, attributes, field):
"""

FIELDS_HOOKS = {
    'redirects_from': {
        'prepare': prepare_redirects_from,
        'transform': transform_redirects_from
    },
    'redirects_to': {
        'prepare': prepare_redirects_to,
        'transform': transform_redirects_to
    },
}

# Set prepare and transform functions for nested inlinks and outlinks
for link_direction in ('inlinks', 'outlinks'):
    for field in filter(lambda i: i.startswith('{}.'.format(link_direction)), QUERY_FIELDS):
        _, _f = field.split('.')
        FIELDS_HOOKS["{}.{}".format(link_direction, _f)] = {
            'prepare': lambda query, es_document, link_direction=link_direction, link_type=_f: prepare_links(query, es_document, link_direction, link_type),
            'transform': lambda query, es_document, attributes, ink_direction=link_direction, link_type=_f: transform_links(query, es_document, attributes, link_direction, link_type)
        }

# Prepare metadata duplicate urls
for field in children_from_field('metadata_duplicate'):
    _, _f = field.split('.')
    FIELDS_HOOKS["metadata_duplicate.{}".format(_f)] = {
        'prepare': lambda query, es_document, field=_f: prepare_links(query, es_document, "metadata_duplicate", field),
        'transform': lambda query, es_document, attributes, field=_f: transform_links(query, es_document, attributes, "metadata_duplicate", field)
    }

# Set default values on nested objects
for nested_field, default in (('inlinks_nb', 0), ('outlinks_nb', 0),
                              ('metadata_nb', 0), ('metadata', []),
                              ('metadata_duplicate_nb', 0)):
    for field in children_from_field(nested_field):
        FIELDS_HOOKS[field] = {
            "default": default
        }


def predicate_not_null(filters):
    """
    Subobject cannot be checked with filter 'exists'
    We need to check the existence of one of the required subobject fields
    """
    if 'properties' in URLS_DATA_MAPPING["urls"]["properties"][filters["field"]]:
        _f = {"or": []}
        for field in URLS_DATA_MAPPING["urls"]["properties"][filters["field"]]["properties"]:
            _f["or"].append({"exists": {"field": "{}.{}".format(filters["field"], field)}})
        return _f
    else:
        return {
            "exists": {
                "field": filters['field']
            }
        }

PREDICATE_FORMATS = {
    'eq': lambda filters: {
        "term": {
            filters['field']: filters['value'],
        }
    },
    'match': lambda filters: {
        "term": {
            filters['field']: filters['value'],
        }
    },
    'starts': lambda filters: {
        "prefix": {
            filters['field']: filters['value'],
        }
    },
    'ends': lambda filters: {
        "regexp": {
            filters['field']: "@%s" % filters['value']
        }
    },
    'contains': lambda filters: {
        "regexp": {
            filters['field']: "@%s@" % filters['value']
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
                "from": filters['value'],
                "include_lower": False
            }
        }
    },
    'lte': lambda filters: {
        "range": {
            filters['field']: {
                "to": filters['value'],
            }
        }
    },
    'lt': lambda filters: {
        "range": {
            filters['field']: {
                "to": filters['value'],
                "include_upper": False
            }
        }
    },
    'not_null': lambda filters: predicate_not_null(filters)
}


def is_boolean_operation_filter(filter_dict):
    return isinstance(filter_dict, dict) and len(filter_dict) == 1 and filter_dict.keys()[0].lower() in ('and', 'or')


class Query(object):

    def __init__(self, es_location, es_index, crawl_id, revision_number, query, start=0, limit=100, sort=('id',)):
        self.es_location = es_location
        self.es_index = es_index
        self.crawl_id = crawl_id
        self.revision_number = revision_number
        self._results = {}
        self._urls_ids = set()
        self.query = query
        self.start = start
        self.limit = limit
        self.sort = sort

    @property
    def results(self):
        self._run()
        for k in self._results['results']:
            yield k

    @property
    def count(self):
        self._run()
        return self._results['count']

    def make_raw_query(self, query, sort=None):
        """
        Transform Botify query to elastic search query
        """
        def has_nested_func(q):
            return {
                "nested": {
                    "path": "tagging",
                    "filter": q
                }
            }

        q = {}

        if sort:
            q['sort'] = sort

        if 'tagging_filters' in query and 'filters' in query:
            q["filter"] = {
                "and": [
                    has_nested_func(self._make_raw_tagging_filters(query['tagging_filters'])),
                    self._make_raw_filters(query['filters'])
                ]
            }
        elif 'tagging_filters' in query:
            q["filter"] = has_nested_func(self._make_raw_tagging_filters(query['tagging_filters']))
        elif 'filters' in query:
            q["filter"] = self._make_raw_filters(query['filters'])
        else:
            pass
        return q

    def _make_raw_tagging_filters(self, filters):
        if is_boolean_operation_filter(filters):
            operator = filters.keys()[0].lower()
            return {operator: self._make_raw_tagging_filters(filters.values()[0])}
        elif isinstance(filters, list):
            return [self._make_raw_tagging_filters(f) for f in filters]
        else:
            field_name = filters.get('field')
            if field_name == "resource_type":
                subfilter = {
                    'and': [
                        {"field": "tagging.resource_type", "predicate": filters.get('predicate'), "not": filters.get('not'), "value": filters.get('value')},
                        {"field": "tagging.rev_id", "value": self.revision_number}
                    ]
                }
                return self._make_raw_filters(subfilter)

            predicate = filters.get('predicate', 'match')
            if filters.get('not', False):
                return {"not": PREDICATE_FORMATS[predicate](filters)}
            else:
                return PREDICATE_FORMATS[predicate](filters)

    def _make_raw_filters(self, filters, has_parent=False):
        if is_boolean_operation_filter(filters):
            operator = filters.keys()[0].lower()
            return {operator: self._make_raw_filters(filters.values()[0], True)}
        elif isinstance(filters, list) and not has_parent:
            return {"and": [self._make_raw_filters(f, True) for f in filters]}
        elif isinstance(filters, list):
            return [self._make_raw_filters(f, True) for f in filters]
        else:
            predicate = filters.get('predicate', 'eq')
            if filters.get('not', False):
                return {"not": PREDICATE_FORMATS[predicate](filters)}
            else:
                return PREDICATE_FORMATS[predicate](filters)

    def _run(self):
        if 'count' in self._results:
            return
        """
        Compute a list of urls depending on parameters

        :param query

        Example of query : 
        {
            "fields": ["url", "id", "metadata.h1"],
            "sort": ["id"],
            "filters": [
                {"field": "resource_type", "predicate": "match", "value": "recette/permalink"}
                {"field": "metadata.h1", "predicate": "match", "value": "recette"}
            ],
        }

        Ex :

        {
            "count": 30,
            "results": [
                {
                    "url": "http://www.site.com",
                    "host": "www.site.com",
                    "resource_type": "homepage"
                },
                {
                    "url": "http://www.site.com/article.html",
                    "host": "www.site.com",
                    "resource_type": "article"
                }
            ]
        }
        """
        query = self.query
        if not 'fields' in query:
            query['fields'] = ('url',)

        # some pages not crawled are stored into ES but should not be returned
        filter_http_code = {'field': 'http_code', 'value': 0, 'predicate': 'gt'}
        if not 'filters' in query:
            query['filters'] = filter_http_code
        elif 'and' in query['filters']:
            query['filters']['and'].append(filter_http_code)
        else:
            query['filters'] = {'and': [filter_http_code, query['filters']]}

        if 'sort' in query:
            sort = query['sort']
        else:
            sort = ('id', )

        s = ElasticSearch(self.es_location)
        alt_results = s.search(self.make_raw_query(query, sort=sort),
                               index=self.es_index,
                               doc_type="crawl_%d" % self.crawl_id,
                               size=self.limit,
                               es_from=self.start)
        if alt_results["hits"]["total"] == 0:
            self._results = {
                "count": 0,
                "results": []
            }
            return

        results = []

        for r in alt_results['hits']['hits']:
            document = {'id': r['_id']}

            for field in query['fields']:
                if field_has_children(field):
                    for child in children_from_field(field):
                        if child in FIELDS_HOOKS and 'prepare' in FIELDS_HOOKS[child]:
                            FIELDS_HOOKS[child]['prepare'](self, r['_source'])
                if field in FIELDS_HOOKS and 'prepare' in FIELDS_HOOKS[field]:
                    FIELDS_HOOKS[field]['prepare'](self, r['_source'])

        """
        Resolve urls ids added in `prepare` functions hooks
        """
        if self._urls_ids:
            urls_es = s.multi_get(self._urls_ids,
                                  index=self.es_index,
                                  doc_type="crawl_%d" % self.crawl_id,
                                  fields=["url", "http_code"])
            self._id_to_url = {int(url['_id']): (url['fields']['url'], url['fields']['http_code']) for url in urls_es['docs'] if url["exists"]}

        for r in alt_results['hits']['hits']:
            document = {}

            for field in query['fields']:
                if field_has_children(field):
                    for child in children_from_field(field):
                        if child in FIELDS_HOOKS and 'transform' in FIELDS_HOOKS[child]:
                            FIELDS_HOOKS[child]['transform'](self, r['_source'], document)
                        else:
                            default_value = FIELDS_HOOKS.get(child, {"default": 0}).get("default", 0)
                            try:
                                value = [reduce(dict.get, child.split("."), r['_source']) or default_value]
                            except:
                                value = [default_value]
                            deep_update(document, reduce(lambda x, y: {y: x}, reversed(child.split('.') + value)))
                elif field in FIELDS_HOOKS and 'transform' in FIELDS_HOOKS[field]:
                    FIELDS_HOOKS[field]['transform'](self, r['_source'], document)
                else:
                    default_value = FIELDS_HOOKS.get(field, {"default": 0}).get("default", 0)
                    if '.' in field:
                        try:
                            value = [reduce(dict.get, field.split("."), r['_source'])]
                        except:
                            value = [default_value]
                        deep_update(document, reduce(lambda x, y: {y: x}, reversed(field.split('.') + value)))
                    else:
                        document[field] = deep_clean(r['_source'][field]) if field in r['_source'] else default_value

            """
            for _f in QUERY_TAGGING_FIELDS:
                if _f in query['fields']:
                    for t in r['_source']['tagging']:
                        if t['rev_id'] == self.revision_number:
                            document[_f] = t[_f]
                            break
            """
            results.append(document)

        self._results = {
            'count': alt_results['hits']['total'],
            'results': results
        }
