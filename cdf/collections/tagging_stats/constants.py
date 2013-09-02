from cdf.streams.mapping import CONTENT_TYPE_INDEX


CROSS_PROPERTIES_COLUMNS = ('host', 'resource_type', 'content_type', 'depth', 'http_code', 'index', 'follow')

COUNTERS_FIELDS = (
    'pages_nb',
    'total_delay_ms',
    'redirections_nb',
    'canonical_filled_nb',
    'canonical_duplicates_nb',
    'canonical_incoming_nb',

    'inlinks_internal_nb',
    'inlinks_internal_nb.total',
    'inlinks_internal_nb.follow',
    'inlinks_internal_nb.follow_unique',
    'inlinks_internal_nb.nofollow',
    'inlinks_internal_nb.nofollow_combinations',
    'inlinks_internal_nb.nofollow_combinations.link',
    'inlinks_internal_nb.nofollow_combinations.link_meta',
    'inlinks_internal_nb.nofollow_combinations.link_meta_robots',
    'inlinks_internal_nb.nofollow_combinations.link_robots',
    'inlinks_internal_nb.nofollow_combinations.meta',
    'inlinks_internal_nb.nofollow_combinations.meta_robots',
    'inlinks_internal_nb.nofollow_combinations.robots',

    'outlinks_internal_nb',
    'outlinks_internal_nb.total',
    'outlinks_internal_nb.follow',
    'outlinks_internal_nb.follow_unique',
    'outlinks_internal_nb.nofollow',
    'outlinks_internal_nb.nofollow_combinations',
    'outlinks_internal_nb.nofollow_combinations.link',
    'outlinks_internal_nb.nofollow_combinations.link_meta',
    'outlinks_internal_nb.nofollow_combinations.link_meta_robots',
    'outlinks_internal_nb.nofollow_combinations.link_robots',
    'outlinks_internal_nb.nofollow_combinations.meta',
    'outlinks_internal_nb.nofollow_combinations.meta_robots',
    'outlinks_internal_nb.nofollow_combinations.robots',

    'outlinks_external_nb',
    'outlinks_external_nb.total',
    'outlinks_external_nb.follow',
    'outlinks_external_nb.nofollow',
    'outlinks_external_nb.nofollow_combinations',
    'outlinks_external_nb.nofollow_combinations.link',
    'outlinks_external_nb.nofollow_combinations.link_meta',
    'outlinks_external_nb.nofollow_combinations.meta',

    'delay_gte_2s',
    'delay_from_1s_to_2s',
    'delay_from_500ms_to_1s',
    'delay_lt_500ms',
    'not_enough_metadata'
)

for ct_txt in CONTENT_TYPE_INDEX.itervalues():
    COUNTERS_FIELDS += ('%s_filled_nb' % ct_txt, '%s_unique_nb' % ct_txt)
