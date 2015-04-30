from __future__ import absolute_import

import unittest
import tempfile
import os.path
import logging
import shutil
import gc

from cdf.utils.kvstore import leveldb, exceptions, will_destroy


logger = logging.getLogger(__name__)


class Base(object):
    def setUp(self):
        self.path = tempfile.mkdtemp()
        self.db = self.cls(self.path)
        self.db.open()

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def test_no_auto_del(self):
        del self.db
        self.db = None
        gc.collect()

        self.assertTrue(os.path.exists(self.path))

    def test_put_get(self):
        db = self.db
        db.put('a', '1')
        db.put('b', '2')
        self.assertEquals(db.get('a'), '1')
        self.assertEquals(db.get('b'), '2')
        db.destroy()

    def test_cannot_destroy_wrong_path(self):
        with tempfile.NamedTemporaryFile() as tmp:
            db = self.cls(tmp.name)
            self.assertTrue(db.db is None)
            with self.assertRaises(exceptions.KVStoreException):
                db.destroy()
            self.assertTrue(os.path.exists(tmp.name))

    def test_will_destroy(self):
        with will_destroy(self.db) as db:
            db.put('1', '1')
        self.assertFalse(os.path.exists(self.path))

    def test_iterator(self):
        for i in range(3):
            self.db.put(str(i), '')
        result = [(k, v) for k, v in self.db.iterator()]
        expected = [('0', ''), ('1', ''), ('2', '')]
        self.assertEquals(result, expected)

    def test_write_batch(self):
        s = iter([(str(i), '') for i in range(3)])
        self.db.batch_write(s, batch_size=2)
        result = [(k, v) for k, v in self.db.iterator()]
        expected = [('0', ''), ('1', ''), ('2', '')]
        self.assertEquals(result, expected)

try:
    from cdf.utils.kvstore import plyvel

    class TestPlyvel(Base, unittest.TestCase):
        cls = plyvel.LevelDB
except ImportError as err:
    logger.warning('cannot load test for plyvel: {}'.format(err))


class TestLeveldb(Base, unittest.TestCase):
    cls = leveldb.LevelDB
