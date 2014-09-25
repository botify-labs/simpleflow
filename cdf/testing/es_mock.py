__all__ = [
    'ELASTICSEARCH_INDEX',
    'CRAWL_ID',
    'CRAWL_NAME',
    'REVISION_ID',
    'get_es_mget_mock'
]


ELASTICSEARCH_INDEX = 'es_mock'
CRAWL_ID = 1
CRAWL_NAME = 'crawl_%d' % CRAWL_ID
REVISION_ID = 1

_MOCK_MGET_RESPONSE = {
    '1:1': ['url1'],
    '1:2': ['url2'],
    '1:3': ['url3'],
    '1:4': ['url4'],
    '1:5': ['url5']
}


def get_es_mget_mock(responses=_MOCK_MGET_RESPONSE, raw=False):
    """Util that generates an mget mock function

    :param responses: mock responses of mget.
        Expected a dict of (id -> document fields values).
        The response will be of form (id, doc, found).
        Ex. retrieve fields are ['url', 'http_code'], then each entry of the
        dict should be like: {'1': ['abc.com', 200]}
    :type responses: dict
    :param raw: if the mock function returns raw ElasticSearch response,
        defaults to False
    :type raw: bool
    :return: mget mock function, to be used with mock.MagicMock()
    :rtype: func
    """
    def mock_raw_mget(**kwargs):
        assert 'body' in kwargs
        body = kwargs['body']
        fields = kwargs.get('_source', [])
        docs = []
        for _id in body['ids']:
            if _id in responses:
                retrived = responses[_id]
                doc = {k: retrived[i] for i, k in enumerate(fields)}

                crt_doc = {
                    u'_type': CRAWL_NAME,
                    u'found': True,
                    u'_index': ELASTICSEARCH_INDEX,
                    u'_source': fields,
                    u'_version': 1,
                    u'_id': _id
                }
                docs.append(crt_doc)
            else:
                docs.append({u'found': False})

        result = {
            u'docs': docs
        }
        return result

    def mock_mget(ids, fields, **kwargs):
        results = []
        for _id in ids:
            if _id in responses:
                retrieved = responses[_id]
                doc = {k: retrieved[i] for i, k in enumerate(fields)}
                results.append((_id, doc, True))
            else:
                # not-found
                results.append((None, None, False))
        return results

    if raw:
        return mock_raw_mget
    else:
        return mock_mget