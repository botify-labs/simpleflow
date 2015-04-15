__author__ = 'zeb'

import unittest

import re
from cdf.utils.regex_checking import check

class BasicTests(unittest.TestCase):
    def test_empty(self):
        s = ""
        self.assertTrue(check(s))

    def test_simple(self):
        s = "toto"
        self.assertTrue(check(s))
