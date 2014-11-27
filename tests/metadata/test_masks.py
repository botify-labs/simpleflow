# -*- coding:utf-8 -*-
import unittest
import logging
import itertools

from cdf.log import logger
from cdf.features.links.helpers.masks import (
    follow_mask,
    list_to_mask,
    _NOFOLLOW_MASKS)

logger.setLevel(logging.DEBUG)


class TestMasks(unittest.TestCase):

    def setUp(self):
        pass

    def test_follow(self):
        # 0 and 8 means follow
        self.assertEquals(follow_mask("0"), ["follow"])
        self.assertEquals(follow_mask("8"), ["follow"])

    def test_nofollow(self):
        # Bitmask test
        for L in range(1, len(_NOFOLLOW_MASKS) + 1):
            for subset in itertools.combinations(_NOFOLLOW_MASKS, L):
                counter = sum(k[0] for k in subset)
                self.assertEquals(follow_mask(str(counter)), [k[1] for k in subset])

    def test_prev_next(self):
        self.assertItemsEqual(follow_mask("32"), ["next", "follow"])
        self.assertItemsEqual(follow_mask("64"), ["prev", "follow"])

        self.assertItemsEqual(follow_mask("33"), ["next", "link"])
        self.assertItemsEqual(follow_mask("68"), ["prev", "robots"])


class TestListToMask(unittest.TestCase):
    def test_follow(self):
        self.assertEqual(0, list_to_mask(['follow']))

    def test_no_follow(self):
        self.assertEqual(4, list_to_mask(["robots"]))
        self.assertEqual(5, list_to_mask(["link", "robots"]))

    def test_prev_next(self):
        self.assertEqual(32, list_to_mask(["next"]))
        self.assertEqual(64, list_to_mask(["prev"]))
        self.assertEqual(37, list_to_mask(["next", "link", "robots"]))
        self.assertEqual(64, list_to_mask(["prev", "follow"]))
