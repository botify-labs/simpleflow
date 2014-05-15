import copy

from elasticsearch import Elasticsearch
from cdf.metadata.url.backend import ELASTICSEARCH_BACKEND
from cdf.query.query_parsing import QueryParser
from cdf.query.result_transformer import transform_result, transform_aggregation_result
from cdf.utils.dict import deep_dict


class Query(object):
    """Abstraction between front-end's botify format query and the ElasticSearch
    API calls

    Front-end construct a query object by passing its botify format query then
    gets the result back from ElasticSearch on `query.count` and `query.results`
    properties.
    """

    def __init__(self, es_location, es_index, es_doc_type, crawl_id, revision_number,
                 botify_query, start=0, limit=100, sort=['id'],
                 backend=ELASTICSEARCH_BACKEND, search_backend=None):

        """Constructor
        search_backend : the search backend to use. If None, use ElasticSearch.
        """
        self.es_location = es_location
        self.es_index = es_index
        self.es_doc_type = es_doc_type
        self.crawl_id = crawl_id
        self.revision_number = revision_number
        self.botify_query = botify_query
        self.fields = None
        self.start = start
        self.limit = limit
        self.sort = sort
        self._count = 0
        self._results = []
        self._aggs = {}
        self.executed = False
        self.backend = backend

        if search_backend:
            self.search_backend = search_backend
        else:
            host, port = self.es_location[7:].split(':')
            self.search_backend = Elasticsearch([{'host': host, 'port': int(port)}])

        self.parser = QueryParser(data_backend=backend)

    @property
    def results(self):
        """Generator of query results"""
        self._run()
        for k in self._results:
            yield k

    @property
    def count(self):
        self._run()
        return self._count

    @property
    def aggs(self):
        self._run()
        return self._aggs

    @staticmethod
    def _get_hit_count(es_result):
        return es_result['hits']['total']

    def _has_agg(self):
        return 'aggs' in self.botify_query

    @property
    def es_query(self):
        return self.parser.get_es_query(self.botify_query, self.crawl_id)

    def _run(self):
        """Launch the process of a ES query
            - Translation of a botify format query to ES search API
            - Query execution
            - Raw result transformation

        Example of query :
        {
            "fields": ["url", "id", "metadata.h1"],
            "sort": ["id"],
            "filters": [
                {"field": "metadata.h1", "predicate": "match", "value": "recette"}
            ],
        }

        Result:

        query.count = 30
        query.results = [
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
        """
        if self.executed:
            return

        # Translation
        es_query = self.es_query

        # Issue a ES search
        temp_results = self.search_backend.search(body=es_query,
                                                  index=self.es_index,
                                                  doc_type=self.es_doc_type,
                                                  routing=self.crawl_id,
                                                  preference=self.crawl_id,
                                                  size=self.limit,
                                                  from_=self.start)

        # Return directly if search has no result
        self._count = self._get_hit_count(temp_results)
        if self._count == 0:
            self._results = []
            self._count = 0
            return

        self._results = []
        self.fields = es_query['_source']

        # make a copy of the fields part
        # need to use `deep_dict` since ES gives a dict with flatten path
        # eg. for field 'a.b', instead of {'a' {'b': 1}}, it gives {'a.b': 1}
        for result in temp_results['hits']['hits']:
            # in transformed ES query, there's always `fields`
            # in this way, ES always response with a` `fields` field containing
            # the selected fields of the result documents

            # the only exception is that the document contains no required fields,
            # in which case we need to create an empty `fields` for default value
            # transformation
            if '_source' in result:
                res = copy.deepcopy(deep_dict(result['_source']))
                self._results.append(res)
            else:
                self._results.append({})

        # Apply transformers
        transform_result(self._results, self, backend=self.backend)
        if self._has_agg():
            self._aggs = transform_aggregation_result(temp_results['aggregations'])

        # Flip flag on execution success
        self.executed = True
