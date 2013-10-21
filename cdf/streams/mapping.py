from .masks import follow_mask


STREAMS_FILES = {
    'urlids': 'patterns',
    'urlinfos': 'infos',
    'urlcontents': 'contents',
    'urlcontentsduplicate': 'contents_duplicate',
    'urllinks': 'outlinks',
    'urlinlinks': 'inlinks',
    'url_out_links_counters': 'outlinks_counters',
    'url_out_redirect_counters': 'outredirect_counters',
    'url_out_canonical_counters': 'outcanonical_counters',
    'url_in_links_counters': 'inlinks_counters',
    'url_in_redirect_counters': 'inredirect_counters',
    'url_in_canonical_counters': 'incanonical_counters',
    'url_suggested_clusters': 'suggest'
}


def str_to_bool(string):
    return string == '1'


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
    'CONTENTS_DUPLICATE': (
        ('id', int),
        ('content_type', int),
        ('filled_nb', int),
        ('duplicates_nb', int),
        ('is_first_url', str_to_bool),
        ('duplicate_urls', lambda k: [int(i) for i in k.split(';')] if k else [])
    ),
    'OUTLINKS_RAW': (
        ('id', int),
        ('link_type', str),
        ('bitmask', int),
        ('dst_url_id', int),
        ('external_url', str)
    ),
    'INLINKS_RAW': (
        ('id', int),
        ('link_type', str),
        ('bitmask', int),
        ('src_url_id', int),
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
    'OUTLINKS_COUNTERS': (
        ('id', int),
        ('follow', follow_mask),
        ('is_internal', str_to_bool),
        ('score', int),
        ('score_unique', int),
    ),
    'OUTREDIRECT_COUNTERS': (
        ('id', int),
        ('is_internal', str_to_bool)
    ),
    'OUTCANONICAL_COUNTERS': (
        ('id', int),
        ('equals', str_to_bool)
    ),
    'INLINKS_COUNTERS': (
        ('id', int),
        ('follow', follow_mask),
        ('score', int),
        ('score_unique', int),
    ),
    'INREDIRECT_COUNTERS': (
        ('id', int),
        ('score', int)
    ),
    'INCANONICAL_COUNTERS': (
        ('id', int),
        ('score', int)
    ),
    'SUGGEST': (
        ('id', int),
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
