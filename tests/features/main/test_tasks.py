import unittest

from cdf.features.main.tasks import get_lang


class TestGetLang(unittest.TestCase):
    def setUp(self):
        self.info_entry = [0, "foo", "en-US", "?"]

    def test_nominal_case(self):
        self.assertEqual("en-US", get_lang(self.info_entry, 2))

    def test_lang_not_set(self):
        self.assertEqual("undef", get_lang(self.info_entry, 4))

    def test_lang_unknown(self):
        self.assertEqual("undef", get_lang(self.info_entry, 3))
