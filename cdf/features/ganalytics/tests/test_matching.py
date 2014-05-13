import unittest

from cdf.features.ganalytics.matching import get_urlid


class TestGetUrlid(unittest.TestCase):
    def setUp(self):
        self.url_to_id = {
            "http://foo.com": 0,
            "http://foo.com/bar": 1,
            "https://foo.com/baz": 2
        }

    def test_nominal_case_http(self):
        entry = ["foo.com/bar"]
        actual_result = get_urlid(entry, self.url_to_id, "http")
        self.assertEqual(1, actual_result)

    def test_nominal_case_https(self):
        entry = ["foo.com/baz"]
        actual_result = get_urlid(entry, self.url_to_id, "https")
        self.assertEqual(2, actual_result)

    def test_ambiguity(self):
        url_to_id = {
            "http://foo.com": 0,
            "https://foo.com": 1,
        }
        entry = ["foo.com"]
        #depending on the preferred protocol, the url id changes
        self.assertEqual(0, get_urlid(entry, url_to_id, "http"))
        self.assertEqual(1, get_urlid(entry, url_to_id, "https"))

    def test_wrong_preferred_protocol(self):
        entry = ["foo.com/baz"]
        actual_result = get_urlid(entry, self.url_to_id, "http")
        #the url id is found even if the preferred protocol is not the actual
        #protocol
        self.assertEqual(2, actual_result)

    def test_unexisting_url(self):
        entry = ["bar.com"]
        actual_result = get_urlid(entry, self.url_to_id, "http")
        self.assertIsNone(actual_result)
