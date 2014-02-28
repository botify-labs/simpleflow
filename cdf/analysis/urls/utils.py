def get_part_id(url_id, first_part_size, part_size):
    """Determine which partition a url_id should go into
    """

    if url_id < first_part_size:
        return 0
    else:
        return (url_id - first_part_size) / part_size + 1


def get_es_id(crawl_id, url_id):
    """Get the composed ElasticSearch _id"""
    return '{}:{}'.format(crawl_id, url_id)


def get_url_id(es_id):
    """Get the url_id from the composed es doc id"""
    return int(es_id.split(':')[1])


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


def is_link_internal(link_mask, dest):
    """Determine if a link is an internal link

    A special case is handled here: if an internal link is blocked by
    robots.txt, our crawler will not allocate it an url id, so the
    destination url id will be -1. This kind of link is treated as an
    internal link.

    Accepts bitmask or decoded nofollow list.

    :param bitmask: the bitmask of the link
    :type link_mask: int, list
    :param dest: the url id of the link destination
    :type dest: int
    """
    is_robots = False
    if isinstance(link_mask, int):
        is_robots = link_mask & 4 == 4
    elif isinstance(link_mask, list) and len(link_mask) == 1:
        is_robots = 'robots' in link_mask

    return dest > 0 or (dest == -1 and is_robots)
