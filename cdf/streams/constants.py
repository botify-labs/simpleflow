def follow_mask(val):
    """
    0 follow, else mask :
    1 link no-follow
    2 meta no-follow
    4 robots no-follow
    8 config no-folfow
    """
    if val == "0":
        return "follow"
    else:
        mask = int(val)
        if 1 & mask == 1:
            return "link_nofollow"
        elif 2 & mask == 2:
            return "meta_nofollow"
        elif 4 & mask == 4:
            return "robots_nofollow"
        elif 8 & mask == 8:
            return "config_nofollow"


STREAMS_FILES = {
    'urlids': 'patterns',
    'urlinfos': 'infos',
    'urlcontents': 'contents',
    'urllinks': 'outlinks',
    'urlinlinks': 'inlinks',
    'url_properties': 'properties'
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

MANDATORY_CONTENT_TYPES = ('title', 'h1', 'description')
