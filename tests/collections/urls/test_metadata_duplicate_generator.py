# -*- coding:utf-8 -*-
import unittest
import logging

from cdf.utils.hashing import string_to_int64


from cdf.log import logger
from cdf.collections.urls.transducers.metadata_duplicate import get_duplicate_metadata

logger.setLevel(logging.DEBUG)


class TestMetadataDuplicateGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple(self):
        stream_contents = iter((
            [1, 2, 1234, 'My first H1'],
            [1, 2, 456, 'My second H1'],
            [1, 3, 7867, 'My H2'],
            [1, 1, 8999, 'My title'],
            [1, 4, 1111, 'My Desc'],
            [2, 2, 1234, 'My first H1'],
            [2, 4, 1111, 'My Desc'],
            [3, 2, 456, 'My second H1'],
            [3, 1, 8999, 'My title'],
            [3, 4, 1111, 'My Desc'],
        ))

        generator = get_duplicate_metadata(stream_contents)
        results = list(generator)
        logger.info(results)
        expected = [
            (1, 1, 1, 2, True, [3]),
            (1, 2, 2, 2, True, [2]),
            (1, 4, 1, 3, True, [2, 3]),
            (2, 2, 1, 2, False, [1]),
            (2, 4, 1, 3, False, [1, 3]),
            (3, 1, 1, 2, False, [1]),
            (3, 2, 1, 0, True, []),
            (3, 4, 1, 3, False, [1, 2])
        ]
        self.assertEquals(results, expected)

    def test_notset_metadata(self):
        """Notset metadata is simply ignored, they do not count in
        `filled_nb` neither
        """
        stream_contents = iter((
            [1, 4, string_to_int64(''), ''],
            [1, 2, 456, 'My second H1'],
            [1, 1, string_to_int64(''), ''],
            [1, 4, 1111, 'My Desc'],
            [2, 4, string_to_int64(''), ''],
            [2, 4, 1111, 'My Desc'],
            [2, 4, 1111, 'My Desc'],
            [3, 1, string_to_int64(''), ''],
        ))

        generator = get_duplicate_metadata(stream_contents)
        results = list(generator)

        expected = [
            (1, 2, 1, 0, True, []),
            (1, 4, 1, 2, True, [2]),
            (2, 4, 2, 2, False, [1]),
        ]
        self.assertEquals(results, expected)
