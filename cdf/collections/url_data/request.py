from pyelasticsearch import ElasticSearch

from .constants import QUERY_URLS_DATA_FIELDS, QUERY_URLS_PROPERTIES_FIELDS

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
    }
}


def is_boolean_operation_filter(filter_dict):
    return isinstance(filter_dict, dict) and len(filter_dict) == 1 and filter_dict.keys()[0].lower() in ('and', 'or')


class UrlRequest(object):

    def __init__(self, es_location, es_index, revision_number):
        self.es_location = es_location
        self.es_index = es_index
        self.revision_number = revision_number

    def make_raw_query(self, query, sort=None):
        """
        Transform Botify query to elastic search query
        """
        def has_parent_query_func(q):
            return {
                "has_parent": {
                    "parent_type": "urls_data",
                    "filter": q
                }
            }

        q = {}

        if sort:
            q['sort'] = sort

        if 'mapping_filters' in query and 'filters' in query:
            q["filter"] = {
                "and": [
                    self._make_raw_mapping_filters(query['mapping_filters']),
                    has_parent_query_func(self._make_raw_filters(query['filters']))
                ]
            }
        elif 'mapping_filters' in query:
            q["filter"] = self._make_raw_mapping_filters(query['mapping_filters'])
        elif 'filters' in query:
            q["filter"] = has_parent_query_func(self._make_raw_filters(query['filters']))
        else:
            pass
        return q

    def _make_raw_mapping_filters(self, filters):
        if is_boolean_operation_filter(filters):
            operator = filters.keys()[0].lower()
            return {operator: self._make_raw_mapping_filters(filters.values()[0])}
        elif isinstance(filters, list):
            return [self._make_raw_mapping_filters(f) for f in filters]
        else:
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
            return [self._make_raw_filters(f) for f in filters]
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
                {"field": "metadata.h1", "predicate": "match", "value": "recette"}
            ],
            "mapping_filters": [
                {"field": "resource_type", "value": "recette/permalink"}
            ]
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
        alt_results = s.search(self.make_raw_query(query, sort=sort), index=self.es_index, doc_type="urls_properties_%d" % self.revision_number, size=limit, es_from=start)

        if alt_results["hits"]["total"] == 0:
            return {
                "count": 0,
                "start": start,
                "limit": limit,
                "results": []
            }

        # Fetch urls data
        results_ids = [r['_id'] for r in alt_results['hits']['hits']]
        urls_documents = s.multi_get(results_ids, index=self.es_index, doc_type="urls_data")
        urls_documents_dict = {d['_id']: d['_source'] for d in urls_documents['docs']}
        results = []

        for r in alt_results['hits']['hits']:
            document = {'id': r['_id']}

            for _f in QUERY_URLS_DATA_FIELDS:
                if _f in query['fields']:
                    if '.' in _f:
                        try:
                            document[_f] = reduce(dict.get, _f.split("."), urls_documents_dict[r['_id']])
                        except:
                            document[_f] = None
                    else:
                        document[_f] = urls_documents_dict[r['_id']][_f]
            for _f in QUERY_URLS_PROPERTIES_FIELDS:
                if _f in query['fields']:
                    document[_f] = r['_source'][_f]

            results.append(document)

        returned_data = {
            'count': alt_results['hits']['total'],
            'start': start,
            'limit': limit,
            'results': results
        }

        return returned_data
