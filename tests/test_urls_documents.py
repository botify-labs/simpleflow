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

        u = UrlsDocuments(ListStream(patterns), ListStream(infos), ListStream([]), ListStream([]), ListStream([]))
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

        u = UrlsDocuments(ListStream(patterns), ListStream(infos), ListStream([]), ListStream([]), ListStream([]))
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

        u = UrlsDocuments(ListStream(patterns), ListStream(infos), ListStream([]), ListStream([]), ListStream([]))
        document = u.__iter__().next()
        self.assertEquals(document['query_string'], '?f1&f2=v2')
        self.assertEquals(document['query_string_keys'], ['f1', 'f2'])
        self.assertEquals(document['query_string_keys_order'], 'f1;f2')
        self.assertEquals(document['query_string_items'], [['f1', ''], ['f2', 'v2']])

    def test_metadata(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        infos = [
            [1, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
        ]

        contents = [
            [1, 'h1', 0, 'My first H1'],
            [1, 'h1', 0, 'My second H1'],
            [1, 'title', 0, 'My title']
        ]

        u = UrlsDocuments(ListStream(patterns), ListStream(infos), ListStream(contents), ListStream([]), ListStream([]))
        document = u.__iter__().next()
        self.assertEquals(document['metadata']['h1'], ['My first H1', 'My second H1'])
        self.assertEquals(document['metadata']['title'], ['My title'])

    def test_metadata_gap(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
            [3, 'http', 'www.site.com', '/path/name3.html', '?f1&f2=v2'],
        ]

        infos = [
            [1, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
            [2, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
            [3, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
        ]

        contents = [
            [1, 'h1', 0, 'My H1'],
            [3, 'h1', 0, 'My H1'],
        ]

        u = UrlsDocuments(ListStream(patterns), ListStream(infos), ListStream(contents), ListStream([]), ListStream([]))
        for document in u:
            if document['id'] in (1, 3):
                self.assertEquals(document['metadata']['h1'], ['My H1'])
            else:
                self.assertTrue('metadata' not in document)

    def test_outlinks(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
            [3, 'http', 'www.site.com', '/path/name3.html', '?f1&f2=v2'],
        ]

        infos = [
            [1, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
            [2, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
            [3, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
        ]

        #format : link_type      follow? src_urlid       dst_urlid       or_external_url
        outlinks = [
            ['a', True, 1, 2, ''],
            ['a', False, 1, 3, ''],
            ['a', True, 1, 4, ''],
            ['a', True, 1, -1, 'http://www.youtube.com'],
            ['a', True, 3, -1, 'http://www.youtube.com'],
        ]

        u = UrlsDocuments(ListStream(patterns), ListStream(infos), ListStream([]), ListStream(outlinks), ListStream([]))
        document = u.__iter__().next()
        logger.info(document)
        self.assertEquals(document['outlinks_internal_nofollow_nb'], 1)
        self.assertEquals(document['outlinks_internal_follow_nb'], 2)
        self.assertEquals(document['outlinks_external_follow_nb'], 1)
        self.assertEquals(document['outlinks_follow_ids'], [2, 4])
        self.assertEquals(document['outlinks_nofollow_ids'], [3])

        # Check that url 2 has no outlinks
        document = u.__iter__().next()
        logger.info(document)
        self.assertTrue('outlinks_internal_nofollow_nb' not in document)
        self.assertTrue('outlinks_internal_follow_nb' not in document)
        self.assertTrue('outlinks_external_nofollow_nb' not in document)
        self.assertTrue('outlinks_external_follow_nb' not in document)

        # Check that url 3 has 1 outlink
        document = u.__iter__().next()
        logger.info(document)
        self.assertTrue('outlinks_internal_nofollow_nb' not in document)
        self.assertTrue('outlinks_internal_follow_nb' not in document)
        self.assertTrue('outlinks_external_nofollow_nb' not in document)
        self.assertEquals(document['outlinks_external_follow_nb'], 1)

    """
    Test outlinks with a stream starting at url 2
    """
    def test_oulinks_start_url2(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
            [3, 'http', 'www.site.com', '/path/name3.html', '?f1&f2=v2'],
        ]

        infos = [
            [1, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
            [2, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
            [3, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
        ]

        #format : link_type      follow? src_urlid       dst_urlid       or_external_url
        outlinks = [
            ['a', True, 2, 3, ''],
            ['a', False, 2, 4, ''],
            ['a', True, 2, 5, ''],
            ['a', True, 2, -1, 'http://www.youtube.com'],
            ['a', True, 3, -1, 'http://www.youtube.com'],
        ]

        u = UrlsDocuments(ListStream(patterns), ListStream(infos), ListStream([]), ListStream(outlinks), ListStream([]))
        documents = list(u)

        # No link for url 1
        document = documents[0]
        logger.info(document)
        self.assertTrue('outlinks_internal_nofollow_nb' not in document)
        self.assertTrue('outlinks_internal_follow_nb' not in document)
        self.assertTrue('outlinks_external_nofollow_nb' not in document)
        self.assertTrue('outlinks_external_follow_nb' not in document)

        # Url 2
        document = documents[1]
        logger.info(document)
        self.assertEquals(document['outlinks_internal_nofollow_nb'], 1)
        self.assertEquals(document['outlinks_internal_follow_nb'], 2)
        self.assertEquals(document['outlinks_external_follow_nb'], 1)
        self.assertEquals(document['outlinks_follow_ids'], [3, 5])
        self.assertEquals(document['outlinks_nofollow_ids'], [4])


    def test_redirect_to(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        infos = [
            [1, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
        ]

        #format : link_type      follow? src_urlid       dst_urlid       or_external_url
        outlinks = [
            ['r301', True, 1, 2, ''],
        ]

        u = UrlsDocuments(ListStream(patterns), ListStream(infos), ListStream([]), ListStream(outlinks), ListStream([]))
        document = u.__iter__().next()
        logger.info(document)
        self.assertEquals(document['redirect_to'], {'url_id': 2, 'http_code': 301})

    def test_inlinks(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        infos = [
            [1, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
        ]

        #format : link_type      follow? dst_urlid       src_urlid
        inlinks = [
            ['a', True, 1, 10],
            ['a', False, 1, 11],
            ['a', True, 1, 12],
        ]

        u = UrlsDocuments(ListStream(patterns), ListStream(infos), ListStream([]), ListStream([]), ListStream(inlinks))
        document = u.__iter__().next()
        logger.info(document)
        self.assertEquals(document['inlinks_nofollow_nb'], 1)
        self.assertEquals(document['inlinks_follow_nb'], 2)
        self.assertEquals(document['inlinks_follow_ids'], [10, 12])
        self.assertEquals(document['inlinks_nofollow_ids'], [11])

    def test_redirect_from(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        infos = [
            [1, 0, datetime(2013, 10, 10, 8, 10, 0), 200, 1200, 303, 456, True],
        ]

        #format : link_type      follow? dst_urlid       src_urlid
        inlinks = [
            ['r301', True, 1, 2],
        ]

        u = UrlsDocuments(ListStream(patterns), ListStream(infos), ListStream([]), ListStream([]), ListStream(inlinks))
        document = u.__iter__().next()
        logger.info(document)
        self.assertEquals(document['redirect_from'], {'url_id': 2, 'http_code': 301})
