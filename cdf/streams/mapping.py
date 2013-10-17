from .masks import follow_mask


STREAMS_FILES = {
    'urlids': 'patterns',
    'urlinfos': 'infos',
    'urlcontents': 'contents',
    'urllinks': 'outlinks',
    'urlinlinks': 'inlinks',
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
        ('hash', str),
        ('txt', str)
    ),
    'OUTLINKS_RAW': (
        ('id', int),
        ('link_type', str),
        ('follow', str),
        ('dst_url_id', str),
        ('external_url', str)
    ),
    'INLINKS_RAW': (
        ('id', int),
        ('link_type', str),
        ('follow', str),
        ('src_url_id', str),
    ),
    'OUTLINKS': (
        ('id', int),
        ('link_type', str),
        ('follow', follow_mask),
        ('dst_url_id', int),
        ('external_url', str)
    ),
    'INLINKS': (
        ('id', int),
        ('link_type', str),
        ('follow', follow_mask),
        ('src_url_id', int),
    ),
    'SUGGEST': (
        ('id', int),
        ('section', str),
        ('type', str),
        ('query_hash', str)
    )
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
