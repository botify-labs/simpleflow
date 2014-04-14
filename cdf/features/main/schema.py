from cdf.metadata.raw.masks import follow_mask

__all__ = ["STREAMS_FILES", "STREAMS_HEADERS",
           "CONTENT_TYPE_INDEX", "CONTENT_TYPE_NAME_TO_ID",
           "MANDATORY_CONTENT_TYPES", "MANDATORY_CONTENT_TYPES_IDS"]


def _str_to_bool(string):
    return string == '1'


STREAMS_FILES = {
    'urlids': 'patterns',
    'urlinfos': 'infos',
    'urlcontents': 'contents',
    'urlcontentsduplicate': 'contents_duplicate',
    'url_suggested_clusters': 'suggest'
}


STREAMS_HEADERS = {
    'PATTERNS': (
        ('id', int),
        ('protocol', str),
        ('host', str),
        ('path', str),
        ('query_string', str),
    ),
    'INFOS': (
        ('id', int),
        ('infos_mask', int),
        ('content_type', str),
        ('depth', int),
        ('date_crawled', int),
        ('http_code', int),
        ('byte_size', int),
        ('delay1', int),
        ('delay2', int),
    ),
    'CONTENTS': (
        ('id', int),
        ('content_type', int),
        ('hash', int),
        ('txt', str)
    ),
    'CONTENTS_DUPLICATE': (
        ('id', int),
        ('content_type', int),
        ('filled_nb', int),
        ('duplicates_nb', int),
        ('is_first_url', _str_to_bool),
        ('duplicate_urls', lambda k: [int(i) for i in k.split(';')] if k else [])
    ),
    'SUGGEST': (
        ('id', int),
        ('query_hash', str)
    ),
}

CONTENT_TYPE_INDEX = {
    1: 'title',
    2: 'h1',
    3: 'h2',
    4: 'description',
    5: 'h3'
}
CONTENT_TYPE_NAME_TO_ID = {v: k for k, v in CONTENT_TYPE_INDEX.iteritems()}

MANDATORY_CONTENT_TYPES = ('title', 'h1', 'description')
MANDATORY_CONTENT_TYPES_IDS = (1, 2, 4)
