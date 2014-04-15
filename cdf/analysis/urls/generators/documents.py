import ujson
from copy import deepcopy
from cdf.core.streams.transformations import group_with
from cdf.metadata.url import ELASTICSEARCH_BACKEND


_DEFAULT_DOCUMENT = ELASTICSEARCH_BACKEND.default_document()


def _clean_document(doc):
    def _recursive_clean(doc, access):
        for k, v in doc.items():
            if isinstance(v, dict):
                _recursive_clean(v, doc[k])
            elif v in (None, [], {}):
                del(access[k])
    _recursive_clean(doc, doc)


def _pre_process_document(pre_processors):
    def func(doc, stream_ids):
        """Init the document and process `urlids` stream
        """
        # init the document with default field values
        doc.update(deepcopy(_DEFAULT_DOCUMENT))
        for p in pre_processors:
            p(doc)

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
        self.right_streams = []
        for stream in streams:
            if stream.stream_type.__class__.__name__ == "PatternsStream":
                self.left_stream = stream
            else:
                self.right_streams.append(stream)

        hooks_processors = {'pre': [], 'post': []}

        for hook in ('pre', 'post'):
            method_name = '{}_process_document'.format(hook)
            if hasattr(self.left_stream.stream_type, method_name):
                hooks_processors[hook].append(getattr(self.left_stream.stream_type, method_name))
            for stream in self.right_streams:
                if hasattr(stream.stream_type, method_name):
                    hooks_processors[hook].append(getattr(stream.stream_type, method_name))

        # `urlids` is the reference stream
        left = (self.left_stream.stream, 0, _pre_process_document(hooks_processors['pre']))
        streams_ref = {
            right_stream.stream_type.__class__.__name__: (
                right_stream.stream,
                right_stream.stream_type.primary_key_idx,
                right_stream.stream_type.process_document
            ) for right_stream in self.right_streams}
        self.generator = group_with(left, final_func=_post_process_document(hooks_processors['post']),
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
