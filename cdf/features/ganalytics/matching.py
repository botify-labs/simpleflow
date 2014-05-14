from cdf.log import logger


def get_urlid(visit_stream_entry,
              url_to_id,
              preferred_protocol):
    """Find the url id corresponding to an entry in a visit stream.
    The function checks if the http and https version of the url exist.
    If only one of them exists, it returns the corresponding url id.
    If both exist, it returns the id corresponding to the preferred protocol
    If none exist, it returns None.
    :param visit_stream_entry: an entry from the visit stream.
    :type visit_stream_entry: list
    :param url_to_id: the dict url -> urlid
    :type url_to_id: dict
    :param preferred_protocol: the protocol to prefer in case
                               the http and https
                               versions of the url exist
    :type preferred_protocol: str
    :returns: int
    """
    url = visit_stream_entry[0]
    #generate candidate url ids
    candidates = []
    for protocol in ["http", "https"]:
        candidate_url = '{}://{}'.format(protocol, url)
        url_id = url_to_id.get(candidate_url, None)
        if url_id is None:
            continue
        candidates.append((protocol, url_id))

    #make a decision
    if len(candidates) == 0:
        return None
    elif len(candidates) == 1:
        protocol, urlid = candidates[0]
        return urlid
    elif len(candidates) == 2:
        #take the urlid corresponding to the preferred protocol
        preferred_candidates = [urlid for protocol, urlid in candidates if
                                protocol == preferred_protocol]
        if len(preferred_candidates) != 1:
            raise ValueError("Could not find only one candidate")
        urlid = preferred_candidates[0]
        return urlid
    return None
