import logging
from elasticsearch import Elasticsearch
from elasticsearch.serializer import TextSerializer

from cdf.compat import json
import cdf.settings
from cdf.utils.stream import chunk
from cdf.utils import discovery


logger = logging.getLogger(__name__)


# TODO(darkjh) use thrift protocol
# TODO(darkjh) reuse client
class EsHandler(object):
    """High level ElasticSearch handler

    :param timeout: base timeout for ElasticSearch operations, in seconds
    :type timeout:
    """
    def __init__(self, es_location, es_index, es_doc_type,
                 timeout=20,
                 host_discovery=getattr(
                     cdf.settings, 'HOST_DISCOVERY', None) or
                 discovery.UrlHosts):
        self.es_location = es_location
        # Beware that there is no automatic mechanism to refresh the list of
        # hosts.
        self._host_discovery = host_discovery()
        self.es_host = self._host_discovery.discover(es_location)

        self.es_client = Elasticsearch(self.es_host, timeout=timeout)
        # ES client with text serializer, for raw bulk index only
        # its timeout is set to 2min
        self.raw_bulk_client = Elasticsearch(
            self.es_host, timeout=120, serializer=TextSerializer())
        self.index = es_index
        self.doc_type = es_doc_type

    def __eq__(self, other):
        return (
            self.es_location == other.es_location
            and self.index == other.index
            and self.doc_type == other.doc_type
        )

    def __repr__(self):
        return '<ES of %s/%s/%s>' % (
            self.es_location, self.index, self.doc_type)

    @classmethod
    def _get_index_action(cls, _id):
        return json.dumps({'index': {'_id': _id}})

    @classmethod
    def _parse_bulk_responses(cls, responses):
        successes, failures, an_error_msg = 0, 0, None
        # parse responses
        err = responses.get('errors')
        if err is False:
            return len(responses['items']), 0, ''

        for item in responses['items']:
            err = item['index' if 'index' in item else 'create'].get('error', False)
            if err:
                failures += 1
                if an_error_msg is None:
                    an_error_msg = err
            else:
                successes += 1

        return successes, failures, an_error_msg

    def raw_bulk_index(self, raw_docs, stats_only=True):
        bulks = []
        loads_failures = 0
        for d in raw_docs:
            try:
                doc = json.loads(d)
            except ValueError:
                # skip document that cause decoding problem
                # usually it's pdf, image or wrong-formatted html
                logger.warn("Json decoding error for document: {} ... "
                            "Document skipped...".format(d))
                loads_failures += 1
                continue
            bulks.append(self._get_index_action(doc['_id']))
            bulks.append(d)
        bulks.append('')  # allows extra '\n' in the end

        endpoint = '/{}/{}/_bulk'.format(self.index, self.doc_type)
        body = '\n'.join(bulks)
        # perform bulk request using special client with text serializer
        _, data_json = self.raw_bulk_client.transport.perform_request(
            'POST', endpoint, body=body)

        if not stats_only:
            return data_json

        s, f, an_error_msg = self._parse_bulk_responses(data_json)
        return s, f + loads_failures, an_error_msg

    def bulk(self, docs, bulk_type='index', stats_only=True, **kwargs):
        bulk_actions = []
        for d in docs:
            action = {bulk_type: {}}
            for key in ('_id', '_index', '_parent', '_percolate', '_routing',
                        '_timestamp', '_ttl', '_type', '_version',):
                if key in d:
                    action[bulk_type][key] = d.pop(key)

            bulk_actions.append(action)
            bulk_actions.append(d.get('_source', d))

        # issue the bulk
        responses = self.es_client.bulk(bulk_actions, **kwargs)

        if not stats_only:
            return responses

        return self._parse_bulk_responses(responses)

    def mget(self, ids, fields, routing, chunk_size=500):
        """Issue a multi get to ElasticSearch, retrieve some fields

            ElasticSearch's `multi-get` end point returns documents like:
            {
                u'_type': doc_type,
                u'found': True,
                u'_index': index_name,
                u'_source': {...},
                u'_version': 1,
                u'_id': _id
            }

        This helper returns a list of parsed multi-get results:
            (id, doc, found)
        id and doc default to `None`, found defaults to False

        Warning: fields will not be validated by our data model, use with care
            and prepare for exceptions

        :param ids: iterable of document id
        :type ids: iterable
        :param fields: fields to retrieve, will not be validated
        :type fields: list
        :param chunk_size: size of each request to ElasticSearch
        :type chunk_size: int
        :param routing: routing param of the request
        :type routing: int | str
        :return: list of retrieved sub documents (dict)
        :rtype: list
        """
        results = []
        for trunk in chunk(ids, chunk_size):
            resolved = self.es_client.mget(
                body={"ids": trunk},
                index=self.index,
                doc_type=self.doc_type,
                routing=routing,
                preference=routing,
                _source=fields
            )
            results.extend(
                [(d.get('_id'), d.get('_source'), d.get('found', False))
                 for d in resolved['docs']]
            )
        return results

    def search(self, body, routing, size, start, search_type=None):
        """
        :param body: the query body
        :type body: dict
        :param routing: routing param of the query
        :type routing: int | str
        :param size: max search results
        :type size: int
        :param start: offset of the first search result
        :type start: int
        :return: raw ElasticSearch search results
        :rtype: dict
        """
        params = {
            'body': body,
            'index': self.index,
            'doc_type': self.doc_type,
            'routing': routing,
            'preference': routing,
            'size': size,
            'from_': start,
        }
        if search_type is not None:
            params['search_type'] = search_type

        return self.es_client.search(
            **params
        )

    def refresh(self):
        """Refresh an index to make operations visible for search

        :return: refresh status per shard (success or fail)
        """
        return self.es_client.indices.refresh(index=self.index)


# TODO maybe put it in a util module
def refresh_index(es_location, es_index):
    """Issues a `refresh` request to ElasticSearch cluster

    :param es_location: ElasticSearch cluster location
    :type es_location: str
    :param es_index: name of the index to refresh
    :type es_index: str
    :return: refresh result
    :rtype: dict
    """
    #here we set a timeout of 300 instead of the default 10, because it often
    #happens that the cluster won't synchronize all data under load or while
    #recovering/relocating shards.. common observed values are 30s as of this
    #writing so we should be safe with 300, and we really don't care if this
    #task takes a few minutes to complete
    #TODO: instrument it so we can graph response times
    es = Elasticsearch(es_location, timeout=300)
    return es.indices.refresh(index=es_index)
