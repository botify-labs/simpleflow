import copy

from cdf.metadata.url.es_backend_utils import ElasticSearchBackend
from cdf.query.query_parsing import QueryParser
from cdf.query.result_transformer import transform_result, transform_aggregation_result
from cdf.utils.dict import deep_dict
from cdf.core.metadata.dataformat import generate_data_format
from cdf.utils.es import EsHandler
from cdf.compat import json


# Compatibility hack
# a fake, all complete feature option is created
# and the complete data format is generated based on it
# this will enable front-end to work as it is now
_ALL_FIELDS = {
    'main': {'lang': True},
    'main_image': None,
    'links': {
        'top_anchors': True, 'prev_next': True, 'page_rank': True,
        'links_to_non_canonical': True,
    },
    'semantic_metadata': {'length': True},
    'sitemaps': None,  # not sure
    'ganalytics': None,
    'rel': None,
    'extract': {},
    'comparison': {}
}
# Add extract fields
for typ in 'sibf':
    for i in range(5):
        _ALL_FIELDS['extract']['extract_%s_%i' % (typ, i)] = True

_FEATURE_OPTION = copy.deepcopy(_ALL_FIELDS)
_FEATURE_OPTION['comparison']['options'] = copy.deepcopy(_ALL_FIELDS)
_DATA_FORMAT = generate_data_format(_FEATURE_OPTION)
_COMPARISON_ES_BACKEND = ElasticSearchBackend(_DATA_FORMAT)


def get_mapping():
    """
    Return full ES mapping
    """
    return json.dumps(_COMPARISON_ES_BACKEND.mapping())


class QueryBuilder(object):
    """Build queries with a specific data format
    """
    def __init__(self, es_location, es_index, es_doc_type,
                 crawl_id, feature_options=None, data_backend=None):
        """Init a query builder

        Ideally a query builder should be used as a factory object.
        For example a query builder is created for each crawl, then
        all subsequent query of this crawl is produced by the query
        builder instance

        :param es_location: ElasticSearch location
        :type es_doc_type: str
        :param es_index: ElasticSearch index
        :type es_index: str
        :param es_doc_type: ElasticSearch document type
        :type es_doc_type: str
        :param crawl_id: crawl id
        :type crawl_id: int
        :param feature_options: feature options of the crawl
        :type feature_options: dict
        :param data_backend: optionally query builder can take a
            data backend directly
        :type data_backend: DataBackend
        """
        if data_backend is not None:
            self.data_backend = data_backend
        else:
            data_format = generate_data_format(feature_options)
            self.data_backend = ElasticSearchBackend(data_format)
        self.es = EsHandler(es_location, es_index, es_doc_type)
        self.crawl_id = crawl_id

    def get_query(self, botify_query, start=0, limit=100, sort=['id']):
        """Produce a query instance

        Params have the exact meaning as those of Query

        :return: query object
        :type: Query
        """
        # currently for compatibility reason
        # the Query object's ctor interface is maintained
        return Query(
            None, None, None, self.crawl_id, botify_query,
            start, limit, sort, backend=self.data_backend,
            es_handler=self.es
        )

    def get_aggs_query(self, botify_query):
        """Produce a query instance purely for aggregation usage

        It means no search result will be returned, only
        aggregation results

        :return: query object
        :type: Query
        """
        # currently for compatibility reason
        # the Query object's ctor interface is maintained
        return Query(
            None, None, None, self.crawl_id, botify_query,
            start=0, limit=0, search_type='count',
            backend=self.data_backend, es_handler=self.es
        )


class Query(object):
    """Abstraction between front-end's botify format query and the ElasticSearch
    API calls

    Front-end construct a query object by passing its botify format query then
    gets the result back from ElasticSearch on `query.count` and `query.results`
    properties.
    """
    def __init__(self, es_location, es_index, es_doc_type, crawl_id,
                 botify_query, start=0, limit=100, sort=['id'],
                 backend=_COMPARISON_ES_BACKEND, es_handler=None,
                 search_type=None, timeout=20, **kwargs):
        """Constructor

        :param es_handler: ES handler to use, if `None`, client need to pass ES
            related params
        :param kwargs: keyword args is maintained for compatibility reason
        """
        self.crawl_id = crawl_id
        self.botify_query = botify_query
        self.fields = None
        self.start = start
        self.limit = limit
        self.sort = sort
        self.search_type = search_type
        self._count = 0
        self._results = []
        self._aggs = []
        self.executed = False
        self.backend = backend

        if es_handler is None:
            self.es_handler = EsHandler(
                es_location, es_index, es_doc_type, timeout=timeout)
        else:
            self.es_handler = es_handler

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
        temp_results = self.es_handler.search(
            body=es_query,
            routing=self.crawl_id,
            size=self.limit,
            start=self.start,
            search_type=self.search_type
        )

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
