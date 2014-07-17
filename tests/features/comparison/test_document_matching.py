import unittest
import mock

from cdf.features.comparison import matching
from cdf.features.comparison.exceptions import UrlKeyDecodingError


class TestUrlKeyCoding(unittest.TestCase):
    @classmethod
    @mock.patch('cdf.features.comparison.constants.SEPARATOR', '|')
    def setUpClass(cls):
        reload(matching)

    def test_url_key_encoding(self):
        url = 'http://www.abc.com/'
        url_id = '15'
        result = matching.encode_url_key(url, url_id)
        expected = url + '|' + url_id
        self.assertEqual(result, expected)

    def test_url_key_decoding_normal(self):
        url_key = 'http://www.abc.com/|1234'
        result = matching.decode_url_key(url_key)
        expected = 'http://www.abc.com/', 1234
        self.assertEqual(result, expected)

    def test_url_key_decoding_separator_in_url(self):
        url_key = 'http://www.ab|c.com/|1234'
        result = matching.decode_url_key(url_key)
        expected = 'http://www.ab|c.com/', 1234
        self.assertEqual(result, expected)

    def test_url_key_decoding_error(self):
        url_key = 'http://www.ab|c.com/'
        self.assertRaises(UrlKeyDecodingError,
                          matching.decode_url_key, url_key)

        url_key = 'http://www.abc.com/|'
        self.assertRaises(UrlKeyDecodingError,
                          matching.decode_url_key, url_key)

        url_key = 'http://www.abc.com/|abc'
        self.assertRaises(UrlKeyDecodingError,
                          matching.decode_url_key, url_key)


class TestConversionTable(unittest.TestCase):
    @classmethod
    @mock.patch('cdf.features.comparison.constants.SEPARATOR', '|')
    def setUpClass(cls):
        reload(matching)

    def test_generate_conversion_table(self):
        ref_stream = iter([
            'a|11',
            'c|33',
            'd|44',
        ])

        new_stream = iter([
            'b|2',
            'c|3',
            'd|4',
            'e|5',
        ])

        result = matching.generate_conversion_table(ref_stream, new_stream)
        expected = {33: 3, 44: 4}
        self.assertEqual(result, expected)


class TestUrlIdCorrection(unittest.TestCase):
    def test_correction(self):
        conversion = {1: 11, 2: 22, 3: 33, 4: 44}
        fields = ['a.b', 'c', 'd', 'e', 'f']
        documents = [
            {
                'c': [],
                'd': [[1, True], [2, True]],
                'a': {},
                'f': {'url_str': 'no_correction_needed'}
            },
            {
                'c': [1, 2, 3],
                'd': [[1, True, 0], [9, True, 1]],
                'a': {
                    'b': 5
                },
                'e': {'url_id': 4}
            }
        ]

        expected = [
            {
                'c': [],
                'd': [[11, True], [22, True]],
                'a': {},
                'f': {'url_str': 'no_correction_needed'}
            },
            {
                'c': [11, 22, 33],
                'd': [[11, True, 0], [-9, True, 1]],
                'a': {
                    'b': -5  # no-match url
                },
                'e': {'url_id': 44}
            }
        ]

        result = list(matching.document_url_id_correction(
            iter(documents), conversion_table=conversion,
            correction_fields=fields))

        self.assertEqual(result, expected)
