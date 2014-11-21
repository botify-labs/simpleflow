import unittest
from cdf.features.main.utils import (
    get_url_to_id_dict_from_stream,
    get_id_to_url_dict_from_stream,
    filter_urlids
)


class TestUtils(unittest.TestCase):
    def setUp(self):
        self.ids = [
            [1, 'http', 'www.site.com', '/path/name.html', ''],
            [2, 'http', 'www.site.com', '/path/name2.html', ''],
            [3, 'http', 'www.site.com', '/path/name3.html', '?sid=54'],
        ]

    def test_get_url_to_id_dict_from_stream(self):
        url_to_id = get_url_to_id_dict_from_stream(iter(self.ids))
        self.assertEquals(
            url_to_id,
            {
                'http://www.site.com/path/name.html': 1,
                'http://www.site.com/path/name2.html': 2,
                'http://www.site.com/path/name3.html?sid=54': 3
            }
        )

    def test_get_id_to_url_dict_from_stream(self):
        id_to_url = get_id_to_url_dict_from_stream(iter(self.ids))
        self.assertEquals(
            id_to_url,
            {
                1: 'http://www.site.com/path/name.html',
                2: 'http://www.site.com/path/name2.html',
                3: 'http://www.site.com/path/name3.html?sid=54'
            }
        )

    def test_get_id_to_url_dict_from_stream_subset(self):
        id_to_url = get_id_to_url_dict_from_stream(iter(self.ids), [1, 3])
        self.assertEquals(
            id_to_url,
            {
                1: 'http://www.site.com/path/name.html',
                3: 'http://www.site.com/path/name3.html?sid=54'
            }
        )

class TestFilterUrlIds(unittest.TestCase):
    def setUp(self):
        self.urlids_stream = iter([
            [0, "http", "host.com", "/url0", ""],
            [1, "http", "host.com", "/url1", ""],
            [2, "http", "host.com", "/url2", ""],
            [3, "http", "host.com", "/url3", ""],
            [4, "http", "host.com", "/url4", ""],
            [5, "http", "host.com", "/url5", ""]
        ])

    def test_nominal_case(self):
        actual_result = filter_urlids([0, 4], self.urlids_stream)
        expected_result = [
            [0, "http", "host.com", "/url0", ""],
            [4, "http", "host.com", "/url4", ""],
        ]
        self.assertEqual(expected_result, list(actual_result))

    def test_unsorted_urlids(self):
        actual_result = filter_urlids([4, 0], self.urlids_stream)
        expected_result = [
            [0, "http", "host.com", "/url0", ""],
            [4, "http", "host.com", "/url4", ""],
        ]
        self.assertEqual(expected_result, list(actual_result))

    def test_no_corresponding_entry(self):
        urlids_stream = iter([
            [0, "http", "host.com", "/url0", ""],
            [2, "http", "host.com", "/url2", ""],
            [3, "http", "host.com", "/url3", ""],
            #4 is missing
            [5, "http", "host.com", "/url5", ""]
        ])

        actual_result = filter_urlids([2, 4, 5], urlids_stream)
        expected_result = [
            [2, "http", "host.com", "/url2", ""],
            [5, "http", "host.com", "/url5", ""],
        ]
        self.assertEqual(expected_result, list(actual_result))

    def test_empty_urlids(self):
        actual_result = filter_urlids([], self.urlids_stream)
        self.assertEqual([], list(actual_result))

    def test_emtpy_urlids_stream(self):
        actual_result = filter_urlids([0, 4], iter([]))
        self.assertEqual([], list(actual_result))
