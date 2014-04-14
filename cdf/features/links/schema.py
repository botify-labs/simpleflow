from .helpers.masks import follow_mask

__all__ = ["STREAMS_FILES", "STREAMS_HEADERS"]


def _str_to_bool(string):
    return string == '1'


STREAMS_FILES = {
    'urllinks': 'outlinks',
    'urlinlinks': 'inlinks',
    'url_out_links_counters': 'outlinks_counters',
    'url_out_redirect_counters': 'outredirect_counters',
    'url_out_canonical_counters': 'outcanonical_counters',
    'url_in_links_counters': 'inlinks_counters',
    'url_in_redirect_counters': 'inredirect_counters',
    'url_in_canonical_counters': 'incanonical_counters',
    'urlbadlinks': 'badlinks',
    'urlbadlinks_counters': 'badlinks_counters'
}


STREAMS_HEADERS = {
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
        ('is_internal', _str_to_bool),
        ('score', int),
        ('score_unique', int),
    ),
    'OUTREDIRECT_COUNTERS': (
        ('id', int),
        ('is_internal', _str_to_bool)
    ),
    'OUTCANONICAL_COUNTERS': (
        ('id', int),
        ('equals', _str_to_bool)
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
    'BADLINKS': (
        ('id', int),
        ('dst_url_id', int),
        ('http_code', int)
    ),
    'BADLINKS_COUNTERS': (
        ('id', int),
        ('http_code', int),
        ('score', int)
    )
}
