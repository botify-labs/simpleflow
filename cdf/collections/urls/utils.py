from .constants import QUERY_FIELDS


def field_has_children(field):
    prefix = field + '.'
    return any(i[:len(prefix)] == prefix for i in QUERY_FIELDS)


def children_from_field(field):
    prefix = field + '.'
    return [i for i in QUERY_FIELDS if i[:len(prefix)] == prefix]


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
