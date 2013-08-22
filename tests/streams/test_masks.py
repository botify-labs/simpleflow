# -*- coding:utf-8 -*-
import unittest
import logging
import itertools

from cdf.log import logger
from cdf.streams.masks import follow_mask, NOFOLLOW_MASKS

logger.setLevel(logging.DEBUG)


class TestMasks(unittest.TestCase):

    def setUp(self):
        pass

    def test_follow(self):
        self.assertEquals(follow_mask("0"), ["follow"])
        for L in range(1, len(NOFOLLOW_MASKS) + 1):
            for subset in itertools.combinations(NOFOLLOW_MASKS, L):
                counter = sum(k[0] for k in subset)
                self.assertEquals(follow_mask(str(counter)), ["nofollow_{}".format(k[1]) for k in subset])
