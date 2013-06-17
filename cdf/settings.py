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
        ('depth', int),
        ('date_crawled', int),
        ('http_code', int),
        ('byte_size', int),
        ('delay1', int),
        ('delay2', int),
        ('gzipped', bool)
    ),
    'CONTENTS': (
        ('id', int),
        ('content_type', str),
        ('hash', str),
        ('txt', str)
    ),
    'OUTLINKS': (
        ('link_type', str),
        ('follow', bool),
        ('id', int),
        ('dst_url_id', int),
        ('external_url', str)
    ),
    'INLINKS': (
        ('link_type', str),
        ('follow', bool),
        ('id', int),
        ('src_url_id', int),
    ),
    'OUTCANONICALS': (
        ('id', int),
        ('dst_url_id', int)
    ),
    'INCANONICALS': (
        ('id', int),
        ('src_url_id', int)
    )
}

CONTENT_TYPE_INDEX = {
    1: 'title',
    2: 'h1',
    3: 'h2',
    4: 'description'
}
