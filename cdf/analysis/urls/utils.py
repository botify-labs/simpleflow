




def query_filter_is_predicate(query):
    """
    :param query: a Botify Url Query
    :type query: dict
    :returns bool
    """
    return isinstance(query, dict) and "field" in query


def merge_queries_filters(*args):
    query = {"and": []}
    for _q in args:
        if query_filter_is_predicate(_q) or "or" in _q:
            query["and"].append(_q)
        elif "and" in _q:
            query["and"] += _q["and"]
    return query