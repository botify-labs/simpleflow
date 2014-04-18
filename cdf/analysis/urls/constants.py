from cdf.features.semantic_metadata.settings import CONTENT_TYPE_NAME_TO_ID


SUGGEST_CLUSTERS = ['mixed']


CLUSTER_TYPE_TO_ID = {
    'pattern': {
        'host': 10,
        'path': 11,
        'qskey': 12,
    },
    'metadata': {
        CONTENT_TYPE_NAME_TO_ID['title']: 20,
        CONTENT_TYPE_NAME_TO_ID['description']: 21,
        CONTENT_TYPE_NAME_TO_ID['h1']: 22,
        CONTENT_TYPE_NAME_TO_ID['h2']: 23,
        CONTENT_TYPE_NAME_TO_ID['h3']: 32
    }
}
