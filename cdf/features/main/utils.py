from operator import itemgetter
from cdf.features.main.streams import IdStreamDef

def get_url_to_id_dict_from_stream(stream):
    """
    Return a dictionnary of urls keys mapping to local url_ids
    """
    field_indexes = IdStreamDef.fields_idx(
        ["id", "protocol", "host", "path", "query_string"]
    )
    d = {}
    for elt in stream:
        url_id, protocol, host, path, query_string = itemgetter(*field_indexes)(elt)
        d[protocol + '://' + ''.join((host, path, query_string))] = url_id
    return d


def get_id_to_url_dict_from_stream(stream):
    """Return a dictionary of url_ids mapping to urls
    """
    return {
        urlid: url for url, urlid in get_url_to_id_dict_from_stream(stream).iteritems()
    }
