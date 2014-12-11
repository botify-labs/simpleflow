import ujson as json
from elasticsearch import Elasticsearch
import requests

from cdf.utils.url import get_domain
from cdf.utils.stream import chunk


# TODO(darkjh) use thrift protocol
class EsHandler(object):
    """High level ElasticSearch handler
    """
    def __init__(self, es_location, es_index, es_doc_type):
        self.es_location = es_location
        self.es_host = self._get_domain(es_location)
        self.es_client = Elasticsearch(self.es_host)
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
    def _get_domain(cls, host):
        if host.startswith('http'):
            return get_domain(host)
        return host

    @classmethod
    def _get_index_action(cls, _id):
        return json.dumps({'index': {'_id': _id}})

    @classmethod
    def _parse_bulk_responses(cls, responses):
        success, fail = 0, 0
        # parse responses
        err = responses.get('errors')
        if err is False:
            return len(responses['items']), 0

        for item in responses['items']:
            err = item['index' if 'index' in item else 'create'].get('error', False)
            if err:
                fail += 1
            else:
                success += 1

        return success, fail

    def raw_bulk_index(self, raw_docs, stats_only=True):
        bulks = []
        for d in raw_docs:
            doc = json.loads(d)
            bulks.append(self._get_index_action(doc['_id']))
            bulks.append(d)
        bulks.append('')  # allows extra '\n' in the end

        endpoint = 'http://{}/{}/{}/_bulk'.format(
            self.es_host, self.index, self.doc_type)
        r = requests.post(endpoint, '\n'.join(bulks))

        if not stats_only:
            return r.content

        return self._parse_bulk_responses(json.loads(r.content))

    def bulk(self, docs, bulk_type='index', **kwargs):
        bulk_actions = []
        for d in docs:
            action = {bulk_type: {}}
            for key in ('_id', '_index', '_parent', '_percolate', '_routing',
                        '_timestamp', '_ttl', '_type', '_version',):
                if key in d:
                    action[bulk_type][key] = d.pop(key)

            bulk_actions.append(action)
            bulk_actions.append(d.get('_source', d))

        if not bulk_actions:
            return {}

        return self.es_client.bulk(bulk_actions, **kwargs)

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

    def search(self, body, routing, size, start):
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
        return self.es_client.search(
            body=body,
            index=self.index,
            doc_type=self.doc_type,
            routing=routing,
            preference=routing,
            size=size,
            from_=start
        )

    def refresh(self):
        """Refresh an index to make operations visible for search

        :return: refresh status per shard (success or fail)
        """
        return self.es_client.indices.refresh(index=self.index)


def bulk(client, docs, bulk_type='index', stats_only=True, **kwargs):
    success, failed = 0, 0
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
    responses = client.bulk(bulk_actions, **kwargs)

    if not stats_only:
        return responses
    # parse responses
    err = responses.get('errors')
    if err is False:
        return len(docs), 0

    for req, item in zip(bulk_actions[::2], responses['items']):
        err = item['index' if '_id' in req['index'] else 'create'].get('error')
        if err:
            failed += 1
        else:
            success += 1

    return success, failed