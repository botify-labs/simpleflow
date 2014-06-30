import unittest
import mock
import StringIO
from cdf.core.streams.base import TemporaryDataset
from cdf.features.ganalytics.matching import (MATCHING_STATUS,
                                              match_analytics_to_crawl_urls_stream,
                                              get_urlid,
                                              has_been_crawled,
                                              is_redirection)

class TestMatchAnalyticsToCrawlUrlsStream(unittest.TestCase):
    @mock.patch("cdf.features.ganalytics.streams.ORGANIC_SOURCES", ["google", "bing"])
    @mock.patch("cdf.features.ganalytics.streams.SOCIAL_SOURCES", ["facebook"])
    def test_nominal_case(self):
        stream = iter([
            ("www.foo.com/bar", "organic", "google", None, 10),
            ("www.foo.com/bar", "organic", "bing", None, 8),
            ("www.foo.com/bar", "referral", "facebook.com", "facebook", 3),
            ("www.foo.com/baz", "organic", "google", None, 2),
            ("www.foo.com/qux", "organic", "google", None, 3),
            ("www.foo.com/qux", "organic", "bing", None, 1)
        ])

        url_to_id = {
            "http://www.foo.com/bar": 1,
            "http://www.foo.com/baz": 3
        }

        urlid_to_http_code = {
            1: "http",
            3: "http"
        }

        dataset = mock.create_autospec(TemporaryDataset)
        ambiguous_urls_file = StringIO.StringIO()

        actual_result = match_analytics_to_crawl_urls_stream(stream, url_to_id,
                                                             urlid_to_http_code,
                                                             dataset,
                                                             ambiguous_urls_file)

        #checking result
        expected_top_ghost_pages = {
            'organic.google': [(3, 'www.foo.com/qux')],
            'social.facebook': [],
            'organic.bing': [(1, 'www.foo.com/qux')],
            'organic.all': [(4, 'www.foo.com/qux')],
            'social.all': [],
        }
        self.assertEqual(expected_top_ghost_pages,
                         actual_result.top_pages)

        expected_ghost_pages_session_count = {
            'organic.google': 3,
            'social.facebook': 0,
            'organic.bing': 1,
            'organic.all': 4,
            'social.all': 0,
        }
        self.assertEqual(expected_ghost_pages_session_count,
                         actual_result.session_count)

        expected_ghost_pages_url_count = {
            'organic.google': 1,
            'social.facebook': 0,
            'organic.bing': 1,
            'organic.all': 1,
            'social.all': 0,
        }
        self.assertEqual(expected_ghost_pages_url_count,
                         actual_result.url_count)

        expected_dataset_append_calls = [
            mock.call(1, "organic", "google", None, 10),
            mock.call(1, "organic", "bing", None, 8),
            mock.call(1, "referral", "facebook.com", "facebook", 3),
            mock.call(3, "organic", "google", None, 2)
        ]
        self.assertEqual(expected_dataset_append_calls,
                         dataset.append.mock_calls)


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
        url = "foo.com/bar"
        actual_result = get_urlid(url, self.url_to_id,
                                  self.id_to_http_code)
        self.assertEqual((2, MATCHING_STATUS.OK), actual_result)

    def test_nominal_case_https(self):
        url = "foo.com/baz"
        actual_result = get_urlid(url, self.url_to_id,
                                  self.id_to_http_code)
        self.assertEqual((3, MATCHING_STATUS.OK), actual_result)

    def test_uncrawled_url(self):
        url = "foo.com/bar"
        id_to_http_code = {2: 0}
        actual_result = get_urlid(url, self.url_to_id, id_to_http_code)
        self.assertEquals((None, MATCHING_STATUS.NOT_FOUND), actual_result)

    def test_ambiguity_uncrawled_url(self):
        id_to_http_code = {
            0: 0,
            1: 200
        }
        url = "foo.com"
        #one of the ambiguous urls has not been crawled,
        #so we return the id of the url that has been crawled
        self.assertEqual((1, MATCHING_STATUS.OK),
                         get_urlid(url, self.url_to_id, id_to_http_code))

    def test_ambiguity_http_redirection(self):
        id_to_http_code = {
            0: 301,
            1: 404
        }

        url = "foo.com"
        #http redirects to https, so we should return https url
        self.assertEqual((1, MATCHING_STATUS.OK),
                         get_urlid(url, self.url_to_id, id_to_http_code))

    def test_ambiguity_http_not_redirection(self):
        id_to_http_code = {
            0: 404,
            1: 301
        }

        url = "foo.com"
        #https a redirection so we should return http url
        self.assertEqual((0, MATCHING_STATUS.OK),
                         get_urlid(url, self.url_to_id, id_to_http_code))

    def test_ambiguity_http_https_not_redirections(self):
        id_to_http_code = {
            0: 200,
            1: 200
        }
        url = "foo.com"
        #http and https are not redirections,
        #so we should return http url (by default)
        self.assertEqual((0, MATCHING_STATUS.AMBIGUOUS),
                         get_urlid(url, self.url_to_id, id_to_http_code))

    def test_unexisting_url(self):
        url = "bar.com"
        actual_result = get_urlid(url, self.url_to_id,
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
