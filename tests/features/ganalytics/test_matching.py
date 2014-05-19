import unittest

from cdf.features.ganalytics.matching import (MATCHING_STATUS,
                                              get_urlid,
                                              has_been_crawled,
                                              is_redirection)

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
        self.assertEqual((2, MATCHING_STATUS.OK), actual_result)

    def test_nominal_case_https(self):
        entry = ["foo.com/baz"]
        actual_result = get_urlid(entry, self.url_to_id,
                                  self.id_to_http_code)
        self.assertEqual((3, MATCHING_STATUS.OK), actual_result)

    def test_uncrawled_url(self):
        entry = ["foo.com/bar"]
        id_to_http_code = {2: 0}
        actual_result = get_urlid(entry, self.url_to_id, id_to_http_code)
        self.assertEquals((None, MATCHING_STATUS.NOT_FOUND), actual_result)

    def test_ambiguity_uncrawled_url(self):
        id_to_http_code = {
            0: 0,
            1: 200
        }
        entry = ["foo.com"]
        #one of the ambiguous urls has not been crawled,
        #so we return the id of the url that has been crawled
        self.assertEqual((1, MATCHING_STATUS.OK),
                         get_urlid(entry, self.url_to_id, id_to_http_code))

    def test_ambiguity_http_redirection(self):
        id_to_http_code = {
            0: 301,
            1: 404
        }

        entry = ["foo.com"]
        #http redirects to https, so we should return https url
        self.assertEqual((1, MATCHING_STATUS.OK),
                         get_urlid(entry, self.url_to_id, id_to_http_code))

    def test_ambiguity_http_not_redirection(self):
        id_to_http_code = {
            0: 404,
            1: 301
        }

        entry = ["foo.com"]
        #https a redirection so we should return http url
        self.assertEqual((0, MATCHING_STATUS.OK),
                         get_urlid(entry, self.url_to_id, id_to_http_code))

    def test_ambiguity_http_https_not_redirections(self):
        id_to_http_code = {
            0: 200,
            1: 200
        }
        entry = ["foo.com"]
        #http and https are not redirections,
        #so we should return http url (by default)
        self.assertEqual((0, MATCHING_STATUS.AMBIGUOUS),
                         get_urlid(entry, self.url_to_id, id_to_http_code))

    def test_unexisting_url(self):
        entry = ["bar.com"]
        actual_result = get_urlid(entry, self.url_to_id,
                                  self.url_to_id)
        self.assertEquals((None, MATCHING_STATUS.NOT_FOUND), actual_result)


class TestHasBeenCrawled(unittest.TestCase):
    def test_crawled_url(self):
        urlid_to_http_code = {
            2: 200,
            3: 301,
            4: 404
        }
        self.assertTrue(has_been_crawled(2, urlid_to_http_code))
        self.assertTrue(has_been_crawled(3, urlid_to_http_code))
        self.assertTrue(has_been_crawled(4, urlid_to_http_code))

    def test_non_crawled_url(self):
        urlid_to_http_code = {2: 0}
        self.assertFalse(has_been_crawled(2, urlid_to_http_code))

    def test_unknown_urlid(self):
        urlid_to_http_code = {2: 0}
        self.assertFalse(has_been_crawled(3, urlid_to_http_code))


class TestIsRedirection(unittest.TestCase):
    def test_redirections(self):
        urlid_to_http_code = {
            3: 300,
            4: 301,
            5: 399,
        }
        self.assertTrue(is_redirection(3, urlid_to_http_code))
        self.assertTrue(is_redirection(4, urlid_to_http_code))
        self.assertTrue(is_redirection(5, urlid_to_http_code))

    def test_not_redirection(self):
        urlid_to_http_code = {
            2: 200,
            3: 404
        }
        self.assertFalse(is_redirection(2, urlid_to_http_code))
        self.assertFalse(is_redirection(3, urlid_to_http_code))

    def test_unknown_urlid(self):
        urlid_to_http_code = {2: 0}
        self.assertFalse(is_redirection(3, urlid_to_http_code))
