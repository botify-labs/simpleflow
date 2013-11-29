from elasticsearch import Elasticsearch

from cdf.constants import URLS_DATA_MAPPING
from cdf.exceptions import ElasticSearchIncompleteIndex
from cdf.utils.dict import deep_update
from cdf.utils.unicode import deep_clean
from cdf.streams.masks import follow_mask
from .constants import QUERY_FIELDS
from .utils import field_has_children, children_from_field


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
                    'url': query._id_to_url.get('{}:{}'.format(query.crawl_id, _r['url_id']))[0],
                    'crawled': True
                }
            })


def prepare_redirects_to(query, es_document, field="redirects_to"):
    if field in es_document and 'url_id' in es_document[field]:
        query._urls_ids.add(es_document[field]['url_id'])


def transform_redirects_to(query, es_document, attributes, field="redirects_to"):
    attributes[field] = None
    if field in es_document:
        if 'url_id' in es_document[field]:
            url, http_code = query._id_to_url.get('{}:{}'.format(query.crawl_id, es_document[field]['url_id']))
            attributes[field] = {
                'url': str(url),
                'crawled': http_code > 0
            }
        elif 'url' in es_document[field]:
            """
            It's an external url
            """
            attributes[field] = {
                'url': es_document[field]['url'],
                'crawled': False
            }


def prepare_canonical_from(query, es_document):
    if 'canonical_from' in es_document:
        query._urls_ids |= set(es_document['canonical_from'])


def transform_canonical_from(query, es_document, attributes):
    attributes['canonical_from'] = []
    if not 'canonical_from' in es_document:
        return
    for url_id in es_document['canonical_from']:
        attributes['canonical_from'].append({
            'url': query._id_to_url.get('{}:{}'.format(query.crawl_id, url_id))[0],
            'crawled': True
        })


def prepare_canonical_to(query, es_document):
    if 'canonical_to' in es_document:
        if 'url_id' in es_document['canonical_to']:
            query._urls_ids.add(es_document['canonical_to']['url_id'])


def transform_canonical_to(query, es_document, attributes):
    attributes['canonical_to'] = None
    if 'canonical_to' in es_document:
        if 'url_id' in es_document['canonical_to']:
            attributes['canonical_to'] = {
                'url': query._id_to_url.get('{}:{}'.format(query.crawl_id, es_document['canonical_to']['url_id']))[0],
                'crawled': True
            }
        else:
            attributes['canonical_to'] = {
                'url': es_document['canonical_to']['url'],
                'crawled': False
            }


def transform_resource_type(query, es_document, attributes):
    for item in es_document["tagging"]:
        if item["rev_id"] == query.revision_number:
            attributes['resource_type'] = item["resource_type"]
            return


def prepare_links(query, es_document, link_direction):
    for link_item in es_document.get(link_direction, []):
        query._urls_ids.add(link_item[0])


def transform_links(query, es_document, attributes, link_direction):
    if not link_direction in attributes:
        attributes[link_direction] = []
    for link_item in es_document.get(link_direction, []):
        mask = follow_mask(link_item[1])
        document_id = '{}:{}'.format(query.crawl_id, link_item[0])
        url, http_code = query._id_to_url.get(document_id, [None, None])
        if not url:
            continue
        if mask != ["follow"]:
            mask = ["nofollow_{}".format(_m) for _m in mask]
        attributes[link_direction].append(
            {
                'url': {
                    'url': str(url),
                    'crawled': http_code > 0
                },
                'status': mask,
                'nb_links': link_item[2]
            }
        )


def prepare_metadata_duplicate(query, es_document, link_type):
    if 'metadata_duplicate' in es_document and link_type in es_document['metadata_duplicate']:
        query._urls_ids |= set(es_document['metadata_duplicate'][link_type])


def transform_metadata_duplicate(query, es_document, attributes, link_type):
    if not 'metadata_duplicate' in attributes:
        attributes['metadata_duplicate'] = {}
    if not link_type in attributes['metadata_duplicate']:
        attributes['metadata_duplicate'][link_type] = []
    if not 'metadata_duplicate' in es_document or not link_type in es_document['metadata_duplicate']:
        return
    for url_id in es_document['metadata_duplicate'][link_type]:
        document_id = '{}:{}'.format(query.crawl_id, url_id)
        url, http_code = query._id_to_url.get(document_id)
        attributes['metadata_duplicate'][link_type].append(
            {
                'url': str(url),
                'crawled': http_code > 0
            }
        )

FIELDS_HOOKS = {
    'redirects_from': {
        'prepare': prepare_redirects_from,
        'transform': transform_redirects_from
    },
    'redirects_to': {
        'prepare': prepare_redirects_to,
        'transform': transform_redirects_to
    },
    'canonical_from': {
        'prepare': prepare_canonical_from,
        'transform': transform_canonical_from
    },
    'canonical_to': {
        'prepare': prepare_canonical_to,
        'transform': transform_canonical_to
    },
    'resource_type': {
        'fields': ["tagging"],
        'transform': transform_resource_type
    },
    'inlinks_internal': {
        'prepare': lambda query, es_document: prepare_links(query, es_document, 'inlinks_internal'),
        'transform': lambda query, es_document, attributes: transform_links(query, es_document, attributes, 'inlinks_internal')
    },
    'outlinks_internal': {
        'prepare': lambda query, es_document: prepare_links(query, es_document, 'outlinks_internal'),
        'transform': lambda query, es_document, attributes: transform_links(query, es_document, attributes, 'outlinks_internal')
    },
}

# Prepare metadata duplicate urls
for field in children_from_field('metadata_duplicate'):
    _, _f = field.split('.')
    FIELDS_HOOKS["metadata_duplicate.{}".format(_f)] = {
        'prepare': lambda query, es_document, field=_f: prepare_metadata_duplicate(query, es_document, field),
        'transform': lambda query, es_document, attributes, field=_f: transform_metadata_duplicate(query, es_document, attributes, field)
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

    def __init__(self, es_location, es_index, es_doc_type, crawl_id, revision_number, query, start=0, limit=100, sort=('id',), search_backend = None):
        """Constructor
        search_backend : the search backend to use. If None, use elasticsearch.
        """
        self.es_location = es_location
        self.es_index = es_index
        self.es_doc_type = es_doc_type
        self.crawl_id = crawl_id
        self.revision_number = revision_number
        self._results = {}
        self._urls_ids = set()
        self.query = query
        self.start = start
        self.limit = limit
        self.sort = sort
        if search_backend :
            self.search_backend = search_backend
        else:
            host, port = self.es_location[7:].split(':')
            self.search_backend = Elasticsearch([{'host': host, 'port': int(port)}])

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
                    self._make_raw_filters(query['filters'], has_parent=True)
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

        default_filters = [
            {'field': 'http_code', 'value': 0, 'predicate': 'gt'},
            {'field': 'crawl_id', 'value': self.crawl_id}
        ]

        if not 'filters' in query:
            query['filters'] = {'and': default_filters}
        elif isinstance(query['filters'], dict) and not any(k in ('and', 'or') for k in query['filters'].keys()):
            query['filters'] = {'and': default_filters + [query['filters']]}
        elif 'and' in query['filters']:
            if isinstance(query['filters']['and'], dict):
                query['filters']['and'] = [query['filters']['and'], default_filters]
            else:
                query['filters']['and'] += default_filters
        elif 'or' in query['filters']:
            query['filters']['and'] = [{'and': default_filters}, {'or': query['filters']['or']}]
            del query['filters']['or']
        else:
            raise Exception('filters are not valid for given query')

        if 'sort' in query:
            sort = query['sort']
        else:
            sort = ('id', )

        alt_results = self.search_backend.search(body=self.make_raw_query(query, sort=sort),
                                                 index=self.es_index,
                                                 doc_type=self.es_doc_type,
                                                 size=self.limit,
                                                 offset=self.start)
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
            urls_es = self.search_backend.mget(body={"ids": list('{}:{}'.format(self.crawl_id, url_id) for url_id in self._urls_ids)},
                                               index=self.es_index,
                                               doc_type=self.es_doc_type,
                                               fields=["url", "http_code"])
            #all referenced urlids should be in elasticsearch index.
            if not all([url["exists"] for url in urls_es['docs']]):
                raise ElasticSearchIncompleteIndex("Missing documents")
            self._id_to_url = {url['_id']: (url['fields']['url'], url['fields']['http_code']) for url in urls_es['docs']}

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
                            deep_update(document, reduce(lambda x, y: {y: x}, reversed(child.split('.') + deep_clean(value))))
                elif field in FIELDS_HOOKS and 'transform' in FIELDS_HOOKS[field]:
                    FIELDS_HOOKS[field]['transform'](self, r['_source'], document)
                else:
                    default_value = FIELDS_HOOKS.get(field, {"default": 0}).get("default", 0)
                    if '.' in field:
                        try:
                            value = [reduce(dict.get, field.split("."), r['_source'])]
                        except:
                            value = [default_value]
                        deep_update(document, reduce(lambda x, y: {y: x}, reversed(field.split('.') + deep_clean(value))))
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
