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