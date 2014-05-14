import unittest

from cdf.features.ganalytics.matching import get_urlid


class TestGetUrlid(unittest.TestCase):
    def setUp(self):
        self.url_to_id = {
            "http://foo.com": 0,
            "https://foo.com": 1,
            "http://foo.com/bar": 2,
            "https://foo.com/baz": 3
        }
        self.id_to_http_code = {
            0: 200,
            1: 200,
            2: 200,
            3: 200
        }

    def test_nominal_case_http(self):
        entry = ["foo.com/bar"]
        actual_result = get_urlid(entry, self.url_to_id,
                                  self.id_to_http_code)
        self.assertEqual(2, actual_result)

    def test_nominal_case_https(self):
        entry = ["foo.com/baz"]
        actual_result = get_urlid(entry, self.url_to_id,
                                  self.id_to_http_code)
        self.assertEqual(3, actual_result)

    def test_uncrawled_url(self):
        entry = ["foo.com/bar"]
        id_to_http_code = {2: 0}
        actual_result = get_urlid(entry, self.url_to_id, id_to_http_code)
        self.assertIsNone(actual_result)

    def test_ambiguity_uncrawled_url(self):
        id_to_http_code = {
            0: 0,
            1: 200
        }
        entry = ["foo.com"]
        #one of the ambiguous urls has not been crawled,
        #so we return the id of the url that has been crawled
        self.assertEqual(1, get_urlid(entry, self.url_to_id, id_to_http_code))

    def test_ambiguity_http_redirection(self):
        id_to_http_code = {
            0: 301,
            1: 404
        }

        entry = ["foo.com"]
        #http redirects to https, so we should return https url
        self.assertEqual(1, get_urlid(entry, self.url_to_id, id_to_http_code))

    def test_ambiguity_http_not_redirection(self):
        id_to_http_code = {
            0: 404,
            1: 301
        }

        entry = ["foo.com"]
        #http is not a redirection so we should return http url (by default)
        self.assertEqual(0, get_urlid(entry, self.url_to_id, id_to_http_code))

    def test_unexisting_url(self):
        entry = ["bar.com"]
        actual_result = get_urlid(entry, self.url_to_id,
                                  self.url_to_id)
        self.assertIsNone(actual_result)
