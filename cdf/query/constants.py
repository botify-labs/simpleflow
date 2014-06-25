from enum import Enum

MGET_CHUNKS_SIZE = 1000

# Basic name prefix for top-level aggregations in translated ES query
QUERY_AGG = 'queryagg'

# When you translate an aggregation query on multiple fields
# (like : "groups": ["http_code", "depth"]) into ES format,
# Each item from the list become a sub-aggregation from the previous one
# As each aggregation must have name, the default name is `subagg`
SUB_AGG = 'subagg'


# Identifier for metrics aggregations
# Since the query format is
# [
#    {"sum": {"field": "my_field"}},
#    "count"
# ]
# And ES format asks for a dictionary,
# We use aggregations prefixed by _METRIC_AGG_PREFIX that will store the total number of aggregations
# + the current aggregation (zero-filled on 2 numbers) to ensure
# the sorting and correctly return results as a list
METRIC_AGG_PREFIX = "metricagg"

# Fields flags to return a specific field type rendering
RENDERING = Enum(
    'Rendering',
    [
        ('URL', 'url'),
        ('IMAGE_URL', 'image_url'),
        # Returns a 2-tuple list of (url, http_code)
        ('URL_HTTP_CODE', 'url_http_code'),
        # Return a dict {"url": {"url": url, "crawled": bool_crawled}, "status": ["follow"]}
        ('URL_LINK_STATUS', 'url_link_status'),
        # Return a dict {"url": url, "crawled": bool_crawled}
        ('URL_STATUS', 'url_status'),
        # Returns a map dict:
        # {'text': ["My text", "My other text", ..], 'nb': [20, 10..]}
        ('STRING_NB_MAP', 'string_nb_map'),
        ('TIME_SEC', 'time_sec'),
        ('TIME_MILLISEC', 'time_millisec'),
        ('TIME_MIN', 'time_min'),
        ('PERCENT', 'percent')
    ]
)

FIELD_RIGHTS = Enum(
    'FieldRights',
    [
        # This field is private and cannot be requested outside
        ('PRIVATE', 'private'),
        # This field can be called in filtering operations
        ('FILTERS', 'filters'),
        # This field can be called in filtering operations
        # but just to check if it exists
        ('FILTERS_EXIST', 'filters_exist'),
        # This field can only be selected for results
        ('SELECT', 'select'),
    ]
)
