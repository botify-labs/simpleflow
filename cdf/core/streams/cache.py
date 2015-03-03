import abc
import tempfile
import marshal
import cbor
import os
from itertools import islice

from cdf.utils.external_sort import split_iterable


marshal_serializer = marshal
cbor_serializer = cbor


class AbstractStreamCache(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def cache(self, iterator):
        """Cache a stream locally
        :param iterator: stream to cache, element of this stream
            should be marshallable
        """
        raise NotImplemented

    @abc.abstractmethod
    def get_stream(self):
        """Get a stream from this cache

        :return: cached stream
        :rtype: iterator
        """
        raise NotImplemented


class FileStreamCache(AbstractStreamCache):
    def __init__(self, tmp_file=None, serializer=marshal_serializer):
        self.tmp_file = tmp_file
        self.chunk_size = 100000
        self.serializer = serializer

    # TODO(darkjh) need a lock between cache and get_stream?
    def cache(self, iterator):
        f = None
        for chunk_elements in split_iterable(iterator, self.chunk_size):
            if f is None:
                f = open(self._get_filepath(), "wb")
            for elem in chunk_elements:
                self.serializer.dump(elem, f)
        if f is not None:
            f.close()

    def _get_filepath(self):
        if self.tmp_file is None:
            self.tmp_file = tempfile.mktemp()
        return self.tmp_file

    def get_stream(self):
        if self.tmp_file is not None:
            with open(self.tmp_file) as f:
                while True:
                    try:
                        yield self.serializer.load(f)
                    except (EOFError, IndexError):
                        break

    def _dump_in_file(self, iterator, f):
        for elem in iterator:
            self.serializer.dump(elem, f)

    def __del__(self):
        """Perform resource cleaning when this object is collected
        """
        if self.tmp_file is not None and os.path.exists(self.tmp_file):
            os.remove(self.tmp_file)


class BufferedStreamCache(AbstractStreamCache):
    def __init__(self, tmp_file=None, buffer_size=100000,
                 serializer=marshal_serializer):
        """Constructor
        :param tmp_file: the path to the file where to cache the stream.
                         if None, a tmp file will be created.
        :type tmp_file: str
        :param buffer_size: the size of the buffer to use
        :type buffer_size:int
        """
        self.tmp_file = tmp_file
        self.buffer_size = buffer_size
        self.buffer = None
        self.file_stream_cache = FileStreamCache(self.tmp_file, serializer)

    def cache(self, iterator):
        self.buffer = self.take(self.buffer_size, iterator)
        self.file_stream_cache.cache(iterator)

    def get_stream(self):
        if self.buffer is not None:
            for elt in self.buffer:
                yield elt
        for elt in self.file_stream_cache.get_stream():
            yield elt

    def take(self, n, iterable):
        """Return first n items of the iterable as a list
        from https://docs.python.org/2/library/itertools.html
        """
        return list(islice(iterable, n))

    def __del__(self):
        self.file_stream_cache.__del__()