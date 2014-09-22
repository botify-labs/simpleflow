__all__ = [
    'ELASTICSEARCH_INDEX',
    'CRAWL_ID',
    'CRAWL_NAME',
    'REVISION_ID',
    'mock_es_mget',
]


ELASTICSEARCH_INDEX = 'es_mock'
CRAWL_ID = 1
CRAWL_NAME = 'crawl_%d' % CRAWL_ID
REVISION_ID = 1

_MOCK_URL_MAPPING = {
    '1:1': 'url1',
    '1:2': 'url2',
    '1:3': 'url3',
    '1:4': 'url4',
    '1:5': 'url5'
}


def mock_es_mget(**kwargs):
    assert 'body' in kwargs
    body = kwargs['body']

    docs = []
    for _id in body['ids']:
        url = _MOCK_URL_MAPPING.get(_id, '')
        fields = {
            u'url': url,
            u'http_code': 0
        }

        crt_doc = {
            u'_type': CRAWL_NAME,
            u'found': True,
            u'_index': ELASTICSEARCH_INDEX,
            u'_source': fields,
            u'_version': 1,
            u'_id': _id
        }
        docs.append(crt_doc)

    # url 6 not indexed for some reason
    docs.append({u'found': False})

    result = {
        u'docs': docs
    }
    return result