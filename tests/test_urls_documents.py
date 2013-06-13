# -*- coding:utf-8 -*-
import unittest
import logging


from cdf.log import logger
from cdf.streams import ListStream
from cdf.urls_documents import UrlsDocuments

logger.setLevel(logging.DEBUG)


class TestUrlsDocuments(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', ''],
            [2, 'http', 'www.site.com', '/path/name2.html', '']
        ]

        infos = [
            [1, 0, 6951326, 200, 1200, 303, 456, 0],
            [2, 1, 6951327, 301, 1000, 303, 456, 0],
        ]

        contents = []

        u = UrlsDocuments(ListStream(patterns), ListStream(infos), ListStream(contents))
        urls = [k for k in u]
        print urls
