CANONICAL_FIELD_TO_FILTERS = {
    "filled": {"field": "canonical.to.url", "predicate": "exists"},
    "not_filled": {"not": {"field": "canonical.to.url", "predicate": "exists"}},
    "equal": {"field": "canonical.to.equal", "value": True},
    "not_equal": {"field": "canonical.to.equal", "value": False},
    "incoming": {"field": "canonical.from.nb", "value": 0, "predicate": "gt"}
}


DELAY_FIELD_TO_FILTERS = {
    'delay_gte_2s': {"field": "delay_last_byte", "value": 2000, "predicate": "gte"},
    'delay_from_1s_to_2s': {"field": "delay_last_byte", "value": [100, 1999], "predicate": "between"},
    'delay_from_500ms_to_1s': {"field": "delay_last_byte", "value": [500, 999], "predicate": "between"},
    'delay_lt_500ms': {"field": "delay_last_byte", "value": 5000, "predicate": "lt"}
}


def get_filters_from_http_code_range(http_code):
    return {"field": "http_code", "value": [http_code, http_code + 99], "predicate": "between"}


def get_filters_from_http_code(http_code):
    pass


def get_filters_from_agg_canonical_field(canonical_field):
    return CANONICAL_FIELD_TO_FILTERS[canonical_field]


def get_filters_from_agg_delay_field(delay_field):
    return DELAY_FIELD_TO_FILTERS[delay_field]
