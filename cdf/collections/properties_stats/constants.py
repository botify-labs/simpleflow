from cdf.streams.constants import CONTENT_TYPE_INDEX

CROSS_PROPERTIES_COLUMNS = ('host', 'resource_type', 'content_type', 'depth', 'index', 'follow')

COUNTERS_FIELDS = (
    'pages_nb',
    'pages_code_ok',
    'pages_code_ko',
    'outlinks_nb',
    'total_delay_ms',
    'redirections_nb',
    'canonical_filled_nb',
    'canonical_duplicates_nb',
    'inlinks_nb',
    'canonical_incoming_nb',
    'inlinks_follow_nb',
    'inlinks_nofollow_nb',
    'delay_gte_2s',
    'delay_gte_1s',
    'delay_gte_500ms',
    'delay_lt_500ms'
)

CROSS_PROPERTIES_META_COLUMNS = ('host', 'resource_type')

META_FIELDS = []
for ct_txt in CONTENT_TYPE_INDEX.itervalues():
    META_FIELDS += ['%s_filled_nb' % ct_txt, '%s_local_unik_nb' % ct_txt, '%s_global_unik_nb' % ct_txt]
