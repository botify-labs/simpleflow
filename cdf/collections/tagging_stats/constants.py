import itertools

from cdf.streams.mapping import CONTENT_TYPE_INDEX
from cdf.streams.masks import NOFOLLOW_MASKS

CROSS_PROPERTIES_COLUMNS = ('host', 'resource_type', 'content_type', 'depth', 'http_code', 'index', 'follow')

COUNTERS_FIELDS = (
    'pages_nb',
    'total_delay_ms',
    'redirections_nb',
    'canonical_filled_nb',
    'canonical_duplicates_nb',
    'canonical_incoming_nb',
    'inlinks_nb',
    'inlinks_follow_nb',
    'outlinks_nb',
    'outlinks_follow_nb',
    'delay_gte_2s',
    'delay_from_1s_to_2s',
    'delay_from_500ms_to_1s',
    'delay_lt_500ms',
    'not_enough_metadata'
)

for ct_txt in CONTENT_TYPE_INDEX.itervalues():
    COUNTERS_FIELDS += ('%s_filled_nb' % ct_txt, '%s_unique_nb' % ct_txt)

# Generate all nofollow combinations possible
for L in range(1, len(NOFOLLOW_MASKS) + 1):
    for subset in itertools.combinations(NOFOLLOW_MASKS, L):
        COUNTERS_FIELDS += ('inlinks_{}_nb'.format('__'.join(('nofollow_{}'.format(k[1]) for k in sorted(subset)))),)
        COUNTERS_FIELDS += ('outlinks_{}_nb'.format('__'.join(('nofollow_{}'.format(k[1]) for k in sorted(subset)))),)
