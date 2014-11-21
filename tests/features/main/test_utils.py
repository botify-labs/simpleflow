import unittest
from cdf.features.main.utils import (
    get_url_to_id_dict_from_stream,
    get_id_to_url_dict_from_stream
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
