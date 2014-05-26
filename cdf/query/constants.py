from enum import Enum

MGET_CHUNKS_SIZE = 1000

QUERY_AGG = 'queryagg'

# When you translate an aggregation query on multiple fields
# (like : "groups": ["http_code", "depth"] into ES format,
# Each item from the list become a subaggregation from the previous one
# As each aggregation must be name, the default name is `subagg`
SUB_AGG = 'subagg'


# Identifier for metrics aggregations
# Since the query format is
# [
#    {"sum": {"field": "my_field"},
#    "count"
# ]
# And ES format asks for a dictionnary,
# We use aggregations prefixed by _METRIC_AGG_PREFIX that will store the total number of aggregations
# + the current aggregation (zero-filled on 2 numbers) to ensure
# the sorting and correctly return results as a list
METRIC_AGG_PREFIX = "metricagg"

# Fields flags to return a specific field type rendering
RENDERING = Enum(
    'Rendering',
    [
        ('URL', 'url'),
        ('URL_HTTP_CODE', 'url_http_code'),  # Returns a 2-tuple list of (url, http_code)
        ('STRING_NB_MAP', 'string_nb_map'),  # Returns a map dict {'text': ["My text", "My other text", ..], 'nb': [20, 10..]}
        ('TIME_SEC', 'time_sec'),
        ('TIME_MIN', 'time_min'),
        ('PERCENT', 'percent')
    ]
)

FIELD_RIGHTS = Enum(
    'FieldRights',
    [
        ('PRIVATE', 'private'),  # This field is private and cannot be requested outside
        ('FILTERS', 'filters'),  # This field can be called in filtering operations
        ('FILTERS_EXIST', 'filters_exist'),  # This field can be called in filtering operations but just to check if it exists
        ('SELECT', 'select'),  # This field can only be selected for results
    ]
)
