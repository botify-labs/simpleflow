from cdf.core.features import Feature


def get_urls_data_format_definition():
    urls_def = {}
    for f in Feature.get_features():
        for stream_def in f.get_streams_def():
            if hasattr(stream_def, 'MAPPING'):
                urls_def.update(stream_def.MAPPING)
    return urls_def
