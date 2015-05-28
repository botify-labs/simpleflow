from copy import deepcopy

from cdf.core.streams.transformations import group_with
from cdf.metadata.url.backend import ELASTICSEARCH_BACKEND
from cdf.features.main.streams import IdStreamDef
from cdf.compat import json


_DEFAULT_DOCUMENT = ELASTICSEARCH_BACKEND.default_document()


def _clean_document(doc):
    def _recursive_clean(doc, access):
        for k, v in doc.items():
            if isinstance(v, dict):
                _recursive_clean(v, doc[k])
            elif v in (None, [], {}):
                del(access[k])
    _recursive_clean(doc, doc)

    # FIXME special rule for page rank prototype
    if len(doc.get('internal_page_rank', {})) == 0:
        doc.pop('internal_page_rank', None)

    # FIXME special rule for extract prototype
    if len(doc.get('extract', {})) == 0:
        doc.pop('extract', None)

    # FIXME special rule for duplicate_query_kvs prototype
    if len(doc.get('duplicate_query_kvs', {})) == 0:
        doc.pop('duplicate_query_kvs', None)


def _pre_process_document(left_stream_def, pre_processors):
    def func(doc, stream):
        """Init the document and process `urlids` stream
        """
        # init the document with default field values
        doc.update(deepcopy(_DEFAULT_DOCUMENT))
        for p in pre_processors:
            p(doc)
        left_stream_def.process_document(doc, stream)
    return func


def _post_process_document(post_processors):
    def func(document):
        for p in post_processors:
            p(document)
        _clean_document(document)
    return func


class UrlDocumentGenerator(object):
    """Aggregates incoming streams, produces a json document for each url

    Format see `cdf.metadata.url` package
    """

    def __init__(self, streams):
        self.streams = streams
        self.right_streams = []
        for stream in streams:
            if isinstance(stream.stream_def, IdStreamDef):
                self.left_stream = stream
            else:
                self.right_streams.append(stream)

        # `urlids` is the reference stream
        left = (self.left_stream, 0, _pre_process_document(self.left_stream.stream_def, self.get_pre_processors()))
        streams_ref = {
            right_stream.stream_def.__class__.__name__: (
                right_stream,
                right_stream.stream_def.field_idx('id'),
                right_stream.stream_def.process_document
            ) for right_stream in self.right_streams}
        self.generator = group_with(left, final_func=_post_process_document(self.get_post_processors()),
                                    **streams_ref)

    def __iter__(self):
        return self.generator

    def next(self):
        return next(self.generator)

    def get_pre_processors(self):
        processors = []
        for s in self.streams:
            if hasattr(s.stream_def, 'pre_process_document'):
                processors.append(s.stream_def.pre_process_document)
        return processors

    def get_post_processors(self):
        processors = []
        for s in self.streams:
            if hasattr(s.stream_def, 'post_process_document'):
                processors.append(s.stream_def.post_process_document)
        return processors

    def save_to_file(self, location):
        for file_type in self.files.iterkeys():
            self.files[file_type].seek(0)
        f = open(location, 'w')
        f.writelines('\n'.join((json.encode(l) for l in self)))
        f.close()
