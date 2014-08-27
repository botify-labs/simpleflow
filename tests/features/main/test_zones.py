import unittest

from cdf.features.main.zones import (
    get_lang,
    generate_zone_stream
)


class TestGetLang(unittest.TestCase):
    def setUp(self):
        self.info_entry = [0, "foo", "en-US", "?"]

    def test_nominal_case(self):
        self.assertEqual("en-US", get_lang(self.info_entry, 2))

    def test_lang_not_set(self):
        self.assertEqual("notset", get_lang(self.info_entry, 4))

    def test_lang_unknown(self):
        self.assertEqual("notset", get_lang(self.info_entry, 3))


class TestGenerateZoneStream(unittest.TestCase):
    def test_nominal_case(self):
        id_stream = iter([
            (1, "http", "foo.com", "/"),
            (2, "https", "foo.com", "/bar"),
            (9, "https", "foo.com", "/baz"),
        ])

        infos_stream = iter([
            (1, None, None, None, None, None, None, None, None, "en-US"),
            (2, None, None, None, None, None, None, None, None, "fr"),
            (9, None, None, None, None, None, None, None, None, "?"),
        ])

        zone_stream = generate_zone_stream(id_stream, infos_stream)

        expected_stream = [
            (1, 'en-US,http'),
            (2, 'fr,https'),
            (9, 'notset,https')
        ]
        self.assertEqual(expected_stream, list(zone_stream))
