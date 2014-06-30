from cdf.log import logger

import collections
import itertools
from cdf.features.ganalytics.constants import TOP_GHOST_PAGES_NB
from cdf.features.ganalytics.streams import RawVisitsStreamDef
from cdf.features.ganalytics.ghost import PagesAggregator

MATCHING_STATUS = collections.namedtuple('MATCHING_STATUS', [
    'OK',  # one corresponding url id has been found
    'AMBIGUOUS',  # more than one corresponding url id have been found
    'NOT_FOUND'  # no corresponding url id has been found
])(
    OK='ok',
    AMBIGUOUS='ambiguous',
    NOT_FOUND='not found'
)


def match_analytics_to_crawl_urls_stream(stream, url_to_id, urlid_to_http_code,
                                         dataset, ambiguous_urls_file):
    #init data structures to save the top ghost pages
    #and the number of sessions for ghost pages
    ghost_pages_aggregator = PagesAggregator(TOP_GHOST_PAGES_NB)
    #precompute field indexes as it would be too long to compute them
    #inside the loop
    fields_list = ["url", "medium", "source", "social_network", "nb"]
    url_idx, medium_idx, source_idx, social_network_idx, sessions_idx = RawVisitsStreamDef.fields_idx(fields_list)
    #get all the entries corresponding the the same url
    for url_without_protocol, entries in itertools.groupby(stream, lambda x: x[url_idx]):
        url_id, matching_status = get_urlid(url_without_protocol,
                                            url_to_id,
                                            urlid_to_http_code)
        if url_id:
            #if url is in the crawl, add its data to the dataset
            for entry in entries:
                dataset_entry = list(entry)
                dataset_entry[0] = url_id
                dataset.append(*dataset_entry)
                #store ambiguous url ids
                if matching_status == MATCHING_STATUS.AMBIGUOUS:
                    line = "\t".join([str(i) for i in entry])
                    line = "{}\n".format(line)
                    line = unicode(line)
                    ambiguous_urls_file.write(line)
        elif matching_status == MATCHING_STATUS.NOT_FOUND:
            #if the url is not in the crawl
            #update the ghost pages aggregator with ALL the corresponding
            #entries.
            ghost_pages_aggregator.update(url_without_protocol, entries)

    return ghost_pages_aggregator


def get_urlid(url,
              url_to_id,
              urlid_to_http_code):
    """Find the url id corresponding to a url without any protocol.
    The function checks if the http and https version of the url exist.
    (ie pages have been crawled)
    If only one of them exists, it returns the corresponding url id.
    If none exist, it returns None.
    If both exist, it returns https id if the http is a redirection
    and http id in all other case.
    The function returns a tuple (urlid, matching_status)
    The first is the urlid that has matched (None if no urlid has been found),
    The second indicates whether or not there was an ambiguity.
    :param visit_stream_entry: an entry from the visit stream.
    :type visit_stream_entry: list
    :param url_to_id: the dict url -> urlid
    :type url_to_id: dict
    :param urlid_to_http_code: a dict urlid -> http code
    :type urlid_to_http_code: dic
    :returns: int
    """
    #generate candidate url ids
    candidates = []
    for protocol in ["http", "https"]:
        candidate_url = '{}://{}'.format(protocol, url)
        url_id = url_to_id.get(candidate_url, None)
        if url_id is None:
            continue
        candidates.append((protocol, url_id))

    #remove candidates that have not been crawled
    candidates = [(protocol, urlid) for protocol, urlid in candidates if
                  has_been_crawled(urlid, urlid_to_http_code)]

    #make a decision
    if len(candidates) == 0:
        return None, MATCHING_STATUS.NOT_FOUND
    elif len(candidates) == 1:
        protocol, urlid = candidates[0]
        return urlid, MATCHING_STATUS.OK
    elif len(candidates) == 2:
        candidates_no_redirection = [
            (protocol, urlid) for protocol, urlid in candidates if
            not is_redirection(urlid, urlid_to_http_code)
        ]
        if len(candidates_no_redirection) == 1:
            protocol, urlid = candidates_no_redirection[0]
            return urlid, MATCHING_STATUS.OK
        else:
            protocol_to_urlid = {protocol: urlid for protocol, urlid in candidates}
            http_urlid = protocol_to_urlid["http"]
            return http_urlid, MATCHING_STATUS.AMBIGUOUS
    return None


def has_been_crawled(url_id, urlid_to_http_code):
    """Determine whether or not a url has been crawled
    :param url_id: the consider url id
    :type url_id: int
    :param urlid_to_http_code: a dict url id -> http code
    :type urlid_to_http_code: dict
    :returns: bool
    """
    http_code = urlid_to_http_code.get(url_id, None)
    if http_code is None:
        return False
    #do not consider urls that have not been crawled
    if http_code == 0:
        return False
    return True


def is_redirection(url_id, urlid_to_http_code):
    """Determine wheter or not a url is a redirection.
    :param url_id: the consider url id
    :type url_id: int
    :param urlid_to_http_code: a dict url id -> http code
    :type urlid_to_http_code: dict
    :returns: bool
    """
    http_code = urlid_to_http_code.get(url_id, None)
    if http_code is None:
        return False
    #do not consider urls that have not been crawled
    if 300 <= http_code and http_code < 400:
        return True
    return False
