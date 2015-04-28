import abc
import shutil

from . import exceptions


class KVStoreManager(object):
    """A factory-like object that returns an kv store instance
    It helps handling DB configurations and DB maintenance ops
    """
    pass


class KVStore(object):
    """An abstraction over different embedded key-value stores
    """
    pass


class LevelDBBase(KVStore):
    """A base abstraction over different LevelDB bindings
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, path):
        self.path = path
        self.db = None

    def _check(self):
        """Check that the DB wrapper object is operational
        """
        if self.db is None:
            raise exceptions.KVStoreException('DB not initiated ...')

    @abc.abstractmethod
    def put(self, key, value):
        """Put a key-value pair
        """
        pass

    @abc.abstractmethod
    def get(self, key):
        """Get a value from a key
        """
        pass

    @abc.abstractmethod
    def open(self, **configs):
        """Open the DB for operations"""
        pass

    @abc.abstractmethod
    def close(self):
        """Close the DB"""
        pass

    def destroy(self):
        """Completely remove the DB with its content"""
        self.close()
        # Please ensure there's a sane value in ``self.path`` as
        # :func:`shutil.rmtree` is recursive.
        shutil.rmtree(self.path)

    @abc.abstractmethod
    def iterator(self):
        """Returns an iterator for key-ordered iteration
        """
        pass

    @abc.abstractmethod
    def batch_write(self, kv_stream, batch_size):
        """Batch write a key-value stream into the DB

        Note it's generally better to configure the DB with large write buffer
        `reopen` can be used upon batch write to change the configuration

        :param kv_stream: a key-value pair stream
        :param batch_size: size of each write batch
        """
        pass

    def reopen(self, **configs):
        """Close and then open the DB, potentially with other configurations
        """
        self._check()
        self.close()
        self.open(**configs)