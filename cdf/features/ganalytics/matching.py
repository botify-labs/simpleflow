from cdf.log import logger


def get_urlid(visit_stream_entry, url_to_id, preferred_protocol):
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
    if not url.startswith('http'):
        http_url = 'http://{}'.format(url)
        https_url = 'https://{}'.format(url)
    http_url_id = url_to_id.get(http_url, None)
    https_url_id = url_to_id.get(https_url, None)

    if http_url_id is not None and https_url_id is None:
        url_id = http_url_id
    elif http_url_id is None and https_url_id is not None:
        url_id = https_url_id
    elif http_url_id is not None and https_url_id is not None:
        #in case of ambiguity, choose the preferred protocol
        if preferred_protocol == "http":
            url_id = http_url_id
        elif preferred_protocol == "https":
            url_id = https_url_id
        else:
            logger.warning("Invalid preferred protocol %s", preferred_protocol)
    else:
        url_id = None
    return url_id
