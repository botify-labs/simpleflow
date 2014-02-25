CANONICAL_FIELD_TO_FILTERS = {
    "filled": {"field": "canonical_to", "predicate": "exists"},
    "not_filled": {"not": {"field": "canonical_to", "predicate": "exists"}},
    "equal": {"field": "canonical_to_equal", "value": True},
    "not_equal": {"field": "canonical_to_equal", "value": False},
    "incoming": {"field": "canonical_from_nb", "value": 0, "predicate": "gt"}
}


DELAY_FIELD_TO_FILTERS = {
    'delay_gte_2s': {"field": "delay2", "value": 2000, "predicate": "gte"},
    'delay_from_1s_to_2s': {
        "and": [
            {"field": "delay2", "value": 1000, "predicate": "gte"},
            {"field": "delay2", "value": 2000, "predicate": "lt"}
        ]
    },
    'delay_from_500ms_to_1s': {
        "and": [
            {"field": "delay2", "value": 500, "predicate": "gte"},
            {"field": "delay2", "value": 1000, "predicate": "lt"}
        ]
    },
    'delay_lt_500ms': {"field": "delay2", "value": 5000, "predicate": "lt"}
}


def get_filters_from_http_code_range(http_code):
    return {
        "and": [
            {"field": "http_code", "value": http_code, "predicate": "gte"},
            {"field": "http_code", "value": http_code + 99, "predicate": "lt"},
        ]
    }


def get_filters_from_http_code(http_code):
    pass


def get_filters_from_agg_canonical_field(canonical_field):
    return CANONICAL_FIELD_TO_FILTERS[canonical_field]


def get_filters_from_agg_delay_field(delay_field):
    return DELAY_FIELD_TO_FILTERS[delay_field]
