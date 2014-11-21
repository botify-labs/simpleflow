from itertools import ifilter, imap
from operator import itemgetter
from cdf.core.streams.utils import group_left
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


def get_id_to_url_dict_from_stream(stream, urlids=None):
    """Return a dictionary of url_ids mapping to urls
    :param stream: the urlids stream (based on IdStreamDef)
    :type stream: iterable
    :param urlids: the list of urlids to keep. If None, keep all the urlids.
    :type urlids: list
    :returns: dict - urlid -> url
    """
    if urlids is not None:
        stream = filter_urlids(urlids, stream)
    return {
        urlid: url for url, urlid in get_url_to_id_dict_from_stream(stream).iteritems()
    }


def filter_urlids(urlids, urlids_stream):
    """Filter a urlids stream. Keep only the elements which urlids are in a
    whitelist.
    :param urlids: the whitelist of urlids as a list of ints
    :type urlids: list
    :param urlids_stream: the input stream (based on IdStreamDef)
    :type urlids_stream: iterable
    :returns: iterable
    """
    urlid_idx = IdStreamDef.field_idx("id")
    urlids = imap(lambda x: [x], sorted(urlids))
    grouped_stream = group_left(
        (urlids_stream, urlid_idx),
        ids=(iter(urlids), 0)
    )
    #keep only entries corresponding to an element in the whitelist
    grouped_stream = ifilter(lambda x: len(x[2]["ids"]) > 0, grouped_stream)
    result = imap(lambda x: x[1], grouped_stream)
    return result
