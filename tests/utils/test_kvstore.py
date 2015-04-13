from __future__ import absolute_import

import unittest
import tempfile
import os.path
import logging
import gc

from cdf.utils.kvstore import leveldb
from cdf.utils.kvstore import exceptions


logger = logging.getLogger(__name__)


class Base(object):
    def setUp(self):
        self.path = tempfile.mkdtemp()
        self.db = self.cls(self.path)
        self.db.open()

    def test_open_del(self):
        del self.db
        self.db = None
        gc.collect()

        self.assertFalse(os.path.exists(self.path))

    def test_put_get(self):
        db = self.db
        db.put('a', '1')
        db.put('b', '2')
        self.assertEquals(db.get('a'), '1')
        self.assertEquals(db.get('b'), '2')

    def test_cannot_destroy_wrong_path(self):
        with tempfile.NamedTemporaryFile() as tmp:
            db = self.cls(tmp.name)
            self.assertTrue(db.db is None)
            with self.assertRaises(exceptions.KVStoreException):
                db.destroy()
            self.assertTrue(os.path.exists(tmp.name))


try:
    from cdf.utils.kvstore import plyvel

    class TestPlyvel(Base, unittest.TestCase):
        cls = plyvel.LevelDB
except ImportError as err:
    logger.warning('cannot load test for plyvel: {}'.format(err))


class TestLeveldb(Base, unittest.TestCase):
    cls = leveldb.LevelDB
