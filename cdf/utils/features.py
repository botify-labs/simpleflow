from importlib import import_module
import os


def get_features():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../features')
    mods = []
    for n in os.listdir(path):
        if os.path.isdir(os.path.join(path, n)):
            mods.append(n)
    return mods


def get_features_modules():
    mods = []
    for f in get_features():
        mods.append(import_module('cdf.features.{}'.format(f)))
    return mods


def get_document_generator_settings(feature):
    """
    Return a tuple of 4 values :
    0 : a list of files to fetch
    1 : a dict of PROCESSORS
    2 : a list of PREPARING_PROCESSORS
    3 : a list of FINAL_PROCESSORS
    """
    gen_doc = import_module(feature.__name__ + '.generators.documents')
    return (
        getattr(gen_doc, 'GENERATOR_FILES', []),
        getattr(gen_doc, 'PROCESSORS', {}),
        getattr(gen_doc, 'PREPARING_PROCESSORS', []),
        getattr(gen_doc, 'FINAL_PROCESSORS', [])
    )


def get_streams_files():
    streams_files = {}
    for f in get_features():
        settings = import_module('cdf.features.{}.settings'.format(f))
        if hasattr(settings, 'STREAMS_FILES'):
            streams_files.update(settings.STREAMS_FILES)
    return streams_files


def get_streams_headers():
    streams_headers = {}
    for f in get_features():
        settings = import_module('cdf.features.{}.settings'.format(f))
        if hasattr(settings, 'STREAMS_HEADERS'):
            streams_headers.update(settings.STREAMS_HEADERS)
    return streams_headers


def get_urls_data_format_definition():
    urls_def = {}
    for f in get_features():
        settings = import_module('cdf.features.{}.settings'.format(f))
        if hasattr(settings, 'URLS_DATA_FORMAT_DEFINITION'):
            urls_def.update(settings.URLS_DATA_FORMAT_DEFINITION)
    return urls_def
