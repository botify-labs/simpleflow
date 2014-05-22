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
        ('TIME_SEC', 'time_sec'),
        ('TIME_MIN', 'time_min'),
        ('PERCENT', 'percent')
    ]
)

# Field is private and won't be displayed publicly
PRIVATE = 'private'
