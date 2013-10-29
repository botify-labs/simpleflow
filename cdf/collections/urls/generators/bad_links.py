from cdf.streams.utils import idx_from_stream

def get_bad_links(stream_infos, stream_outlinks):
    """
    (url_src_id, url_dest_id, error_http_code)
    """
    # Resolve indexes
    http_code_idx = idx_from_stream('infos', 'http_code')
    url_id_idx = idx_from_stream('infos', 'id')
    dest_url_idx = idx_from_stream('outlinks', 'dst_url_id')
    src_url_idx = idx_from_stream('outlinks', 'id')
    link_type_idx = idx_from_stream('outlinks', 'link_type')

    # Find all bad code pages
    # (url_id -> error_http_code)
    bad_code = {}
    for info in stream_infos:
        http_code = info[http_code_idx]
        if (http_code >= 300):
            bad_code[info[url_id_idx]] = http_code

    # Iterator over outlinks and extract all normal links whose destination
    # is in the *bad_code* dict
    for outlink in stream_outlinks:
        dest = outlink[dest_url_idx]
        link_type = outlink[link_type_idx]
        if link_type == 'a' and dest in bad_code:
            yield (outlink[src_url_idx], dest, bad_code[dest])
