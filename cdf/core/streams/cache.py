import abc
import tempfile
import marshal
import os

from cdf.log import logger
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
    def __init__(self, tmp_file=None, buffer_size=100000):
        self.tmp_file = tmp_file

        self.buffer = None
        self.buffer_size = buffer_size

    # TODO(darkjh) need a lock between cache and get_stream?
    def cache(self, iterator):
        #handle special case where buffer size is null
        if self.buffer_size == 0:
            with open(self._get_filepath(), "wb") as f:
                for element in iterator:
                    marshal.dump(element, f)
            return

        self.buffer = None
        f = None
        for chunk_elements in split_iterable(iterator, self.buffer_size):
            if self.buffer is None:
                self.buffer = list(chunk_elements)
            else:
                if f is None:
                    f = open(self._get_filepath(), "wb")
                    logger.info("Creating file")
                for elem in self.buffer:
                    marshal.dump(elem, f)
                self.buffer = list(chunk_elements)
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
        if self.buffer is not None:
            for elt in self.buffer:
                yield elt

    def _dump_in_file(self, iterator, f):
        for elem in iterator:
            marshal.dump(elem, f)

    def __del__(self):
        """Perform resource cleaning when this object is collected
        """
        if self.tmp_file is not None:
            os.remove(self.tmp_file)


