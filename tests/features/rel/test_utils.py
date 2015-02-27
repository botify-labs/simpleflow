import unittest
from cdf.features.rel.utils import (
    is_lang_valid,
    get_country,
    is_country_valid
)


class TestUtils(unittest.TestCase):
    def setUp(self):
        pass

    def test_is_lang_valid(self):
        self.assertTrue(is_lang_valid('en'))
        self.assertTrue(is_lang_valid('en-us'))
        self.assertFalse(is_lang_valid('us'))

    def test_get_country(self):
        self.assertEquals(get_country('en-US'), 'US')
        self.assertEquals(get_country('en-Fr'), 'FR')
        self.assertEquals(get_country('en-uu'), 'UU')
        self.assertIsNone(get_country('en'))
        self.assertIsNone(get_country('en-uuu'))

    def test_is_country_valid(self):
        self.assertTrue(is_country_valid('US'))
        self.assertTrue(is_country_valid('FR'))
        self.assertFalse(is_country_valid('us'))
        self.assertFalse(is_country_valid('Fr'))
        self.assertFalse(is_country_valid('uu'))
        self.assertFalse(is_country_valid('uuu'))


