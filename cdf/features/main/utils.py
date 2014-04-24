def get_url_to_id_dict_from_stream(stream):
    """
    Return a dictionnary of urls keys mapping to local url_ids
    """
    d = {}
    for url_id, protocol, host, path, query_string in stream:
        d[protocol + '://' + ''.join((host, path, query_string))] = url_id
    return d
