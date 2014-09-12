# -*- coding:utf-8 -*-
import unittest
import logging

from cdf.log import logger
from cdf.features.semantic_metadata.metadata_duplicate import (
    get_duplicate_metadata,
    get_context_aware_duplicate_metadata,
    notset_hash_value,
)

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
            (1, 1, 2, True, [3]),
            (1, 2, 2, True, [2]),
            (1, 4, 3, True, [2, 3]),
            (2, 2, 2, False, [1]),
            (2, 4, 3, False, [1, 3]),
            (3, 1, 2, False, [1]),
            (3, 2, 0, True, []),
            (3, 4, 3, False, [1, 2])
        ]
        self.assertEquals(results, expected)

    def test_notset_metadata(self):
        """Notset metadata is simply ignored, they do not count in
        `filled_nb` neither
        """
        stream_contents = iter((
            [1, 4, notset_hash_value, ''],
            [1, 2, 456, 'My second H1'],
            [1, 1, notset_hash_value, ''],
            [1, 4, 1111, 'My Desc'],
            [2, 4, notset_hash_value, ''],
            [2, 4, 1111, 'My Desc'],
            [2, 4, 1111, 'My Desc'],
            [3, 1, notset_hash_value, ''],
        ))

        generator = get_duplicate_metadata(stream_contents)
        results = list(generator)

        expected = [
            (1, 2, 0, True, []),
            (1, 4, 2, True, [2]),
            (2, 4, 2, False, [1]),
        ]
        self.assertEquals(results, expected)


class TestZoneAwareMetadataDuplicateGenerator(unittest.TestCase):
    def setUp(self):
        self.stream_contents = iter((
            [1, 2, 1234, 'My first H1'],
            [1, 3, 7867, 'My H2'],
            [1, 1, 8999, 'My title'],
            [1, 4, 1111, 'My Desc'],
            [2, 2, 1234, 'My first H1'],
            [2, 4, 1111, 'My Desc'],
            [3, 1, 8999, 'My title'],
            [3, 2, 456, 'My second H1'],
            [3, 4, 1111, 'My Desc'],
        ))

    def test_strategic_urls(self):

        stream_zones = iter([
            (1, "en-US,https"),
            (2, "en-US,https"),
            (3, "en-US,https")
        ])

        stream_strategic_urls = iter([
            (1, True, 0),
            (2, False, 0),
            (3, True, 0)
        ])

        generator = get_context_aware_duplicate_metadata(self.stream_contents,
                                                      stream_zones,
                                                      stream_strategic_urls)
        results = list(generator)
        expected = [
            (1, 1, 2, True, [3]),
            (1, 2, 0, True, []),
            (1, 4, 2, True, [3]),
            (3, 1, 2, False, [1]),
            (3, 2, 0, True, []),
            (3, 4, 2, False, [1])
        ]
        self.assertEquals(results, expected)

    def test_zones(self):
        stream_zones = iter([
            (1, "en-US,https"),
            (2, "en-US,http"),
            (3, "en-US,https")
        ])

        stream_strategic_urls = iter([
            (1, True, 0),
            (2, True, 0),
            (3, True, 0)
        ])

        generator = get_context_aware_duplicate_metadata(self.stream_contents,
                                                      stream_zones,
                                                      stream_strategic_urls)
        results = list(generator)
        expected = [
            (1, 1, 2, True, [3]),
            (1, 2, 0, True, []),
            (1, 4, 2, True, [3]),
            (2, 2, 0, True, []),
            (2, 4, 0, True, []),
            (3, 1, 2, False, [1]),
            (3, 2, 0, True, []),
            (3, 4, 2, False, [1])
        ]
        self.assertEquals(results, expected)
