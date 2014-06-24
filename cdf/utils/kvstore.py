import plyvel
import os

_DEFAULT_BATCH_SIZE = 10000  # 10k


class KVStoreException(Exception):
    pass


class KVStoreManager(object):
    """A factory-like object that returns an kv store instance
    It helps handling DB configurations and DB maintenance ops
    """
    pass


class KVStore(object):
    """An abstraction over different embedded key-value stores
    """
    pass


# TODO abstract methods into base class
class LevelDB(KVStore):
    def __init__(self, path):
        self.path = path
        self.db = None

    def _check(self):
        """Check that the DB wrapper object is operational
        """
        if self.db is None:
            raise KVStoreException('DB not initiated ...')

    def open(self, **configs):
        """Open the DB
        """
        if os.path.exists(self.path):
            self.db = plyvel.DB(self.path, **configs)
        else:
            self.db = plyvel.DB(self.path, create_if_missing=True, **configs)

    def close(self):
        """Close the DB
        """
        self._check()
        self.db.close()
        self.db = None

    def destroy(self):
        """Close and remove the whole DB (all data is lost)
        """
        self.db.close()
        plyvel.destroy_db(self.path)

    def reopen(self, **configs):
        """Close and then open the DB, potentially with other configurations
        """
        self._check()
        self.close()
        self.open(**configs)

    def batch_write(self, kv_stream, batch_size=_DEFAULT_BATCH_SIZE):
        """Batch write a key-value stream into the DB

        Note it's generally better to configure the DB with large write buffer
        `reopen` can be used upon batch write to change the configuration

        :param kv_stream: a key-value pair stream
        :param batch_size: size of each write batch
        """
        self._check()
        count = 0
        wb = self.db.write_batch()
        for k, v in kv_stream:
            count += 1
            wb.put(str(k), str(v))
            if count % batch_size == 0 and count > 0:
                wb.write()
                wb = self.db.write_batch()
        wb.write()

    def iterator(self):
        """Returns an iterator for key-ordered iteration
        """
        self._check()
        # iteration means we do a full pass on the data
        # but no random lookup, cache is not relevant in
        # this case
        return self.db.iterator(fill_cache=False)

    def put(self, key, value):
        """Put a key-value pair
        """
        self._check()
        self.db.put(key, value)

    def get(self, key):
        """Get a value from a key
        """
        self._check()
        return self.db.get(key)


# TODO study the python binding of RocksDB
class RocksDB(KVStore):
    pass