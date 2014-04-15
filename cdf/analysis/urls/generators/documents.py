import ujson
from copy import deepcopy
from itertools import izip
from cdf.analysis.urls.utils import is_link_internal
from cdf.log import logger
from cdf.metadata.raw import (STREAMS_HEADERS, CONTENT_TYPE_INDEX,
                              MANDATORY_CONTENT_TYPES)
from cdf.core.streams.transformations import group_with
from cdf.core.streams.exceptions import GroupWithSkipException
from cdf.core.streams.utils import idx_from_stream
from cdf.metadata.raw.masks import list_to_mask
from cdf.utils.date import date_2k_mn_to_date
from cdf.utils.hashing import string_to_int64
from cdf.metadata.url import ELASTICSEARCH_BACKEND


# TODO refactor into an ORM fashion
#   data format => hierarchy of classes (ORM)
#   ORM objects knows how to index themselves to ES
# maybe ElasticUtils will be a good reference


_DEFAULT_DOCUMENT = ELASTICSEARCH_BACKEND.default_document()


def _clean_document(doc):
    def _recursive_clean(doc, access):
        for k, v in doc.items():
            if isinstance(v, dict):
                _recursive_clean(v, doc[k])
            elif v in (None, [], {}):
                del(access[k])
    _recursive_clean(doc, doc)


def _extract_stream_fields(stream_identifier, stream):
    """
    :param stream_identifier: stream's id, like 'ids', 'infos'
    :return: a dict containing `field: value` mapping
    """
    return {field[0]: value for field, value in
            izip(STREAMS_HEADERS[stream_identifier], stream)}


def _process_main_stream(preparing_processors):
    def func(doc, stream_ids):
        """Init the document and process `urlids` stream
        """
        # init the document with default field values
        doc.update(deepcopy(_DEFAULT_DOCUMENT))

        # simple information about each url
        doc.update(_extract_stream_fields('PATTERNS', stream_ids))
        doc['url'] = doc['protocol'] + '://' + ''.join(
            (doc['host'], doc['path'], doc['query_string']))
        doc['url_hash'] = string_to_int64(doc['url'])

        query_string = stream_ids[4]
        if query_string:
            # The first character is ? we flush it in the split
            qs = [k.split('=') if '=' in k else [k, '']
                  for k in query_string[1:].split('&')]
            doc['query_string_keys'] = [q[0] for q in qs]

        for p in preparing_processors:
            p(doc)

    return func


def _process_final(final_processors):
    def func(document):
        for p in final_processors:
            p(document)
        _clean_document(document)
    return func


class UrlDocumentGenerator(object):
    """Aggregates incoming streams, produces a json document for each url

    Format see `cdf.metadata.url` package
    """
    def __init__(self, stream_patterns, processors, preparing_processors, final_processors, **kwargs):
        self.stream_patterns = stream_patterns
        self.streams = kwargs
        self.processors = processors
        self.preparing_processors = preparing_processors
        self.final_processors = final_processors

        # `urlids` is the reference stream
        left = (self.stream_patterns, 0, _process_main_stream(self.preparing_processors))
        streams_ref = {key: (self.streams[key], idx_from_stream(key, 'id'),
                             self.processors[key])
                       for key in self.streams.keys()}
        self.generator = group_with(left, final_func=_process_final(self.final_processors),
                                    **streams_ref)

    def __iter__(self):
        return self.generator

    def next(self):
        return next(self.generator)

    def save_to_file(self, location):
        for file_type in self.files.iterkeys():
            self.files[file_type].seek(0)
        f = open(location, 'w')
        f.writelines('\n'.join((ujson.encode(l) for l in self)))
        f.close()
