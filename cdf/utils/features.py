from cdf.core.features import Feature


def get_urls_data_format_definition():
    urls_def = {}
    for f in Feature.get_features():
        urls_def.update(f.urls_data_format_definition)
    return urls_def
