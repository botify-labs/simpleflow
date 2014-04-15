import os
import inspect
import gzip
from importlib import import_module

from cdf.core.streams.base import StreamBase
from cdf.core.streams.caster import Caster
from cdf.core.streams.utils import split_file


class Feature(object):

    @classmethod
    def get_features(cls,):
        if hasattr(cls, 'FEATURES'):
            return cls.FEATURES

        cls.FEATURES = []
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../features')
        for n in os.listdir(path):
            if os.path.isdir(os.path.join(path, n)):
                mod = import_module('cdf.features.{}.settings'.format(n))
                feature = Feature(
                    identifier=n,
                    name=getattr(mod, "NAME", None),
                    description=getattr(mod, "DESCRIPTION", None),
                    streams_files=getattr(mod, "STREAMS_FILES", None),
                    streams_headers=getattr(mod, "STREAMS_HEADERS", None),
                    urls_data_format_definition=getattr(mod, "URLS_DATA_FORMAT_DEFINITION", None),
                )
                cls.FEATURES.append(feature)
        return cls.FEATURES

    def __init__(self, identifier, name, description, streams_files, streams_headers, urls_data_format_definition):
        self.identifier = identifier
        self.name = name
        self.description = description
        self.streams_files = streams_files
        self.streams_headers = streams_headers
        self.urls_data_format_definition = urls_data_format_definition

    def get_streams_objects(self):
        """
        Return streams from the current feature
        """
        obj = []
        try:
            streams = import_module('cdf.features.{}.streams'.format(self.identifier))
        except ImportError:
            return []
        else:
            methods = inspect.getmembers(streams, predicate=inspect.isclass)
            for method_name, klass in methods:
                if type(klass) == type(StreamBase):
                    obj.append(klass())
            return obj

    def get_streams_objects_processing_document(self):
        """
        Return all streams needed to compute urls documents
        """
        obj = []
        for s in self.get_streams_objects():
            if hasattr(s, 'process_document'):
                obj.append(s)
        return obj
