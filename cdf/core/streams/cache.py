import abc
import tempfile
import marshal
import os
from itertools import islice

from cdf.utils.external_sort import split_iterable


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


class MarshalStreamCache(AbstractStreamCache):
    def __init__(self, tmp_file=None):
        self.tmp_file = tmp_file
        self.chunk_size = 100000

    # TODO(darkjh) need a lock between cache and get_stream?
    def cache(self, iterator):
        f = None
        for chunk_elements in split_iterable(iterator, self.chunk_size):
            if f is None:
                f = open(self._get_filepath(), "wb")
            for elem in chunk_elements:
                marshal.dump(elem, f)
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
                        yield marshal.load(f)
                    except EOFError:
                        break

    def _dump_in_file(self, iterator, f):
        for elem in iterator:
            marshal.dump(elem, f)

    def __del__(self):
        """Perform resource cleaning when this object is collected
        """
        if self.tmp_file is not None:
            os.remove(self.tmp_file)


class BufferedMarshalStreamCache(AbstractStreamCache):
    """A cache that buffers the first element in memory
    so that if the stream is small, no data is written to disk
    """
    def __init__(self, tmp_file=None, buffer_size=100000):
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
        self.marshal_stream_cache = MarshalStreamCache(self.tmp_file)

    def cache(self, iterator):
        self.buffer = self.take(self.buffer_size, iterator)
        self.marshal_stream_cache.cache(iterator)

    def get_stream(self):
        if self.buffer is not None:
            for elt in self.buffer:
                yield elt
        for elt in self.marshal_stream_cache.get_stream():
            yield elt

    def take(self, n, iterable):
        """Return first n items of the iterable as a list
        from https://docs.python.org/2/library/itertools.html
        """
        return list(islice(iterable, n))
