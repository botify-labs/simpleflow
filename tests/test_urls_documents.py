# -*- coding:utf-8 -*-
import unittest
import logging
from datetime import datetime


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
        ]

        infos = [
            [1, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
        ]

        contents = []

        u = UrlsDocuments(ListStream(patterns), ListStream(infos), ListStream(contents))
        document = u.__iter__().next()
        document_expected = {'id': 1,
                             'date_crawled': '2013-10-10 08:10:00',
                             'protocol': 'http',
                             'host': 'www.site.com',
                             'path': '/path/name.html',
                             'query_string': '',
                             'delay1': 303,
                             'byte_size': 1200,
                             'depth': 0,
                             'http_code': 200,
                             'gzipped': True,
                             'delay2': 456,
                             }

        self.assertEquals(document, document_expected)

    def test_query_string(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1=v1&f2=v2'],
        ]

        infos = [
            [1, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
        ]

        contents = []

        u = UrlsDocuments(ListStream(patterns), ListStream(infos), ListStream(contents))
        document = u.__iter__().next()
        self.assertEquals(document['query_string'], '?f1=v1&f2=v2')
        self.assertEquals(document['query_string_keys'], ['f1', 'f2'])
        self.assertEquals(document['query_string_keys_order'], 'f1;f2')
        self.assertEquals(document['query_string_items'], [['f1', 'v1'], ['f2', 'v2']])

    def test_query_string_without_value(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        infos = [
            [1, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
        ]

        contents = []

        u = UrlsDocuments(ListStream(patterns), ListStream(infos), ListStream(contents))
        document = u.__iter__().next()
        self.assertEquals(document['query_string'], '?f1&f2=v2')
        self.assertEquals(document['query_string_keys'], ['f1', 'f2'])
        self.assertEquals(document['query_string_keys_order'], 'f1;f2')
        self.assertEquals(document['query_string_items'], [['f1', ''], ['f2', 'v2']])
