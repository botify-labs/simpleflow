import unittest
from cdf.features.rel.utils import (
    is_lang_valid,
    is_region_valid,
    extract_lang_and_region
)


class TestUtils(unittest.TestCase):
    def setUp(self):
        pass

    def test_is_lang_valid(self):
        self.assertTrue(is_lang_valid('en'))
        self.assertTrue(is_lang_valid('en-us'))
        self.assertFalse(is_lang_valid('us'))

    def test_extract_lang_and_region(self):
        self.assertTrue(extract_lang_and_region('x-default'), [None, None])
        self.assertTrue(extract_lang_and_region('en-US'), ['en', 'US'])
        self.assertTrue(extract_lang_and_region('en'), ['en', None])
        self.assertTrue(extract_lang_and_region('badlang'), ['badlang', None])
        self.assertTrue(extract_lang_and_region('badlang-'), ['badlang', None])
        self.assertTrue(extract_lang_and_region('badlang-badcountry-'), ['badlang', 'badcountry'])

    def test_is_region_valid(self):
        self.assertTrue(is_region_valid('us'))
        self.assertTrue(is_region_valid('fr'))
        self.assertFalse(is_region_valid('uu'))
        self.assertFalse(is_region_valid('uuu'))


