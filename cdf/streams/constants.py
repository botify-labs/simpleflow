STREAMS_FILES = {
    'urlids': 'patterns',
    'urlinfos': 'infos',
    'urlcontents': 'contents',
    'urllinks': 'outlinks',
    'urlinlinks': 'inlinks'
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
    'OUTLINKS': (
        ('id', int),
        ('link_type', str),
        ('follow', bool),
        ('dst_url_id', int),
        ('external_url', str)
    ),
    'INLINKS': (
        ('id', int),
        ('link_type', str),
        ('follow', bool),
        ('src_url_id', int),
    ),
    'PROPERTIES': (
        ('id', int),
        ('resource_type', str)
    )
}

CONTENT_TYPE_INDEX = {
    1: 'title',
    2: 'h1',
    3: 'h2',
    4: 'description'
}
