from enum import Enum

NAME = "duplicate_query_kvs"
DESCRIPTION = "URLs with the same query key/values in a different order"
ORDER = 100

GROUPS = Enum(
    'Groups',
    [
        ("duplicate_query_kvs", "same-query-KVs URLs"),
    ]
)
