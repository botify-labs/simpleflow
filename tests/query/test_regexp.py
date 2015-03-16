import unittest
from cdf.query.regexp import normalize_regexp


class TestNormalize(unittest.TestCase):
    def test_normalize_anchors(self):
        result = normalize_regexp('^abc')
        self.assertEqual(result, 'abc.*')

        result = normalize_regexp('^abc$')
        self.assertEqual(result, 'abc')

        result = normalize_regexp('abc$')
        self.assertEqual(result, '.*abc')

        result = normalize_regexp('abc')
        self.assertEqual(result, '.*abc.*')