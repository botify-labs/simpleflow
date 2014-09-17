import abc
import tempfile
import marshal
import os


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
        if tmp_file is None:
            self.tmp_file = tempfile.mktemp()
        else:
            self.tmp_file = tmp_file

    # TODO(darkjh) need a lock between cache and get_stream?
    def cache(self, iterator):
        with open(self.tmp_file, 'wb') as f:
            for elem in iterator:
                marshal.dump(elem, f)

    def get_stream(self):
        with open(self.tmp_file) as f:
            while True:
                try:
                    yield marshal.load(f)
                except EOFError:
                    break

    def __del__(self):
        """Perform resource cleaning when this object is collected
        """
        os.remove(self.tmp_file)