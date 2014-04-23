def get_url_to_id_dict_from_stream(stream):
    """
    Return a dictionnary of urls keys mapping to local url_ids
    """
    d = {}
    for entry in stream:
        d[entry[1] + '://' + ''.join(entry[2:])] = entry[0]
    return d
