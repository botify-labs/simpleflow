from pyelasticsearch import ElasticSearch
from collections import defaultdict

from cdf.utils.dict import deep_update, flatten_dict
from .constants import QUERY_URLS_FIELDS, QUERY_TAGGING_FIELDS, QUERY_URLS_IDS

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
    'not_null': lambda filters: {
        "bool": {
            "must_not": {
                "missing": {
                    "field": filters['field'],
                    "existence": True,
                    "null_value": True
                }
            }
        }
    }
}


def is_boolean_operation_filter(filter_dict):
    return isinstance(filter_dict, dict) and len(filter_dict) == 1 and filter_dict.keys()[0].lower() in ('and', 'or')


class UrlRequest(object):

    def __init__(self, es_location, es_index, crawl_id, revision_number):
        self.es_location = es_location
        self.es_index = es_index
        self.crawl_id = crawl_id
        self.revision_number = revision_number

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

    def _make_raw_filters(self, filters):
        if is_boolean_operation_filter(filters):
            operator = filters.keys()[0].lower()
            return {operator: self._make_raw_filters(filters.values()[0])}
        elif isinstance(filters, list):
            return {"and": [self._make_raw_filters(f) for f in filters]}
        else:
            predicate = filters.get('predicate', 'eq')
            if filters.get('not', False):
                return {"not": PREDICATE_FORMATS[predicate](filters)}
            else:
                return PREDICATE_FORMATS[predicate](filters)

    def query(self, query, start=0, limit=100, sort=('id',)):
        """
        Return a list of urls depending on parameters

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
            "total": 30,
            "start": 0,
            "limit": 100,
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

        if not 'fields' in query:
            query['fields'] = ('url',)

        if 'sort' in query:
            sort = query['sort']
        else:
            sort = ('id', )

        results = {}
        s = ElasticSearch(self.es_location)
        alt_results = s.search(self.make_raw_query(query, sort=sort),
                               index=self.es_index,
                               doc_type="crawl_%d" % self.crawl_id,
                               size=limit,
                               es_from=start)

        if alt_results["hits"]["total"] == 0:
            return {
                "count": 0,
                "start": start,
                "limit": limit,
                "results": []
            }

        results = []
        urls_ids = set()

        for r in alt_results['hits']['hits']:
            document = {'id': r['_id']}

            for _f in QUERY_URLS_FIELDS:
                if _f in query['fields']:
                    if '.' in _f:
                        try:
                            value = [reduce(dict.get, _f.split("."), r['_source'])]
                        except:
                            value = [0]
                        deep_update(document, reduce(lambda x, y: {y: x}, reversed(_f.split('.') + value)))
                    else:
                        if _f in r['_source']:
                            document[_f] = r['_source'][_f]
                        else:
                            document[_f] = None
            for _f in QUERY_TAGGING_FIELDS:
                if _f in query['fields']:
                    for t in r['_source']['tagging']:
                        if t['rev_id'] == self.revision_number:
                            document[_f] = t[_f]
                            break
            results.append(document)

        # If document contains fields with url_ids, we return a list (url_id, real_url) instead
        urls_ids = set()
        for i, result in enumerate(results):
            result_flatten = flatten_dict(result)
            for key, value in result_flatten.iteritems():
                if key in QUERY_URLS_IDS:
                    if isinstance(value, list):
                        urls_ids |= set(value)
                    else:
                        urls_ids.add(value)

        # If urls ids are found, we make a request to fetch those urls
        if urls_ids:
            urls_es = s.multi_get(urls_ids,
                                  index=self.es_index,
                                  doc_type="crawl_%d" % self.crawl_id,
                                  fields=["url"])
            urls = {int(url['_id']): url['fields']['url'] for url in urls_es['docs']}
            for i, result in enumerate(results):
                for field in QUERY_URLS_IDS:
                    try:
                        _urls_ids = reduce(dict.get, field.split("."), results[i])
                    except:
                        _urls_ids = None
                    if _urls_ids:
                        tmp_urls = []
                        for _url_id in _urls_ids:
                            tmp_urls.append(urls[_url_id])
                        deep_update(results[i], reduce(lambda x, y: {y: x}, reversed(field.split('.') + [tmp_urls])))

                for redirect in ('redirect_from', 'redirect_to'):
                    if redirect in results[i]:
                        for k, _entry in enumerate(results[i][redirect]):
                            results[i][redirect][k]['url'] = urls[_entry['url_id']]

        returned_data = {
            'count': alt_results['hits']['total'],
            'start': start,
            'limit': limit,
            'results': results
        }

        return returned_data
