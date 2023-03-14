from __future__ import annotations

import unittest

from swf.querysets.base import BaseQuerySet


class TestBaseQuerySet(unittest.TestCase):
    def setUp(self):
        self.base_qs = BaseQuerySet()

    def tearDown(self):
        pass

    def test_get_method_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.base_qs.get()

    def test_get_or_create_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.base_qs.get()

    def test_filter_method_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.base_qs.filter()

    def test_all_method_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.base_qs.all()

    def test_create_method_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.base_qs.create()
