import unittest
import os

from cdf.core.streams.cache import MarshalStreamCache


class TestStreamCache(unittest.TestCase):
    def setUp(self):
        self.data = [
            [1, 2, 'a'],
            [4, 5, 'b'],
            [7, 8, 'c']
        ]

    def test_harness(self):
        cache = MarshalStreamCache()
        cache.cache(iter(self.data))

        # first consume
        stream = cache.get_stream()
        self.assertEqual(list(stream), self.data)
        # second consume
        stream = cache.get_stream()
        self.assertEqual(list(stream), self.data)

    def test_file_cleaning(self):
        path = '/tmp/cachefile'

        def test_cache():
            cache = MarshalStreamCache(path)
            cache.cache(iter(self.data))

        test_cache()
        self.assertFalse(os.path.exists(path))