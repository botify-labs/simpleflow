# -*- coding:utf-8 -*-

import unittest

from swf.models import BaseModel


class TestBaseModel(unittest.TestCase):

    def setUp(self):
        self.obj = BaseModel()

    def tearDown(self):
        pass

    def test__diff_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.obj._diff()

    def test_exists_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            dummy = self.obj.exists

    def test_save_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.obj.save()

    def test_delete_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.obj.delete()
