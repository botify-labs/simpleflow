# -*- coding:utf-8 -*-
import unittest
import logging
from datetime import datetime


from cdf.log import logger
from cdf.collections.urls.generators.documents import UrlDocumentGenerator

logger.setLevel(logging.DEBUG)


class TestUrlDocumentGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple(self):
        patterns = (
            [1, 'http', 'www.site.com', '/path/name.html', ''],
        )

        infos = (
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        )

        u = UrlDocumentGenerator(iter(patterns), infos=iter(infos))
        document = u.__iter__().next()
        document_expected = {'id': 1,
                             'date_crawled': '2000-01-01T00:01:00',
                             'url': 'http://www.site.com/path/name.html',
                             'url_hash': 5539870621365162490,
                             'protocol': 'http',
                             'host': 'www.site.com',
                             'path': '/path/name.html',
                             'content_type': 'text/html',
                             'gzipped': True,
                             'query_string': '',
                             'delay1': 303,
                             'byte_size': 1200,
                             'depth': 0,
                             'http_code': 200,
                             'delay2': 456,
                             'metadata_nb': {'description': 0, 'h1': 0, 'h2': 0, 'title': 0},
                             'meta_noindex': False,
                             'meta_nofollow': False,
                             }

        self.assertEquals(document, (1, document_expected))

    def test_query_string(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1=v1&f2=v2'],
        ]

        infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        ]

        u = UrlDocumentGenerator(iter(patterns), infos=iter(infos))
        document = u.__iter__().next()[1]
        self.assertEquals(document['query_string'], '?f1=v1&f2=v2')
        self.assertEquals(document['query_string_keys'], ['f1', 'f2'])
        self.assertEquals(document['query_string_keys_order'], 'f1;f2')
        self.assertEquals(document['query_string_items'], [['f1', 'v1'], ['f2', 'v2']])

    def test_query_string_without_value(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        ]

        u = UrlDocumentGenerator(iter(patterns), infos=iter(infos))
        document = u.__iter__().next()[1]
        self.assertEquals(document['query_string'], '?f1&f2=v2')
        self.assertEquals(document['query_string_keys'], ['f1', 'f2'])
        self.assertEquals(document['query_string_keys_order'], 'f1;f2')
        self.assertEquals(document['query_string_items'], [['f1', ''], ['f2', 'v2']])

    def test_metadata(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        ]

        contents = [
            [1, 2, 0, 'My first H1'],
            [1, 2, 0, 'My second H1'],
            [1, 1, 0, 'My title']
        ]

        u = UrlDocumentGenerator(iter(patterns), infos=iter(infos), contents=iter(contents))
        document = u.__iter__().next()[1]
        self.assertEquals(document['metadata']['h1'], ['My first H1', 'My second H1'])
        self.assertEquals(document['metadata']['title'], ['My title'])

    def test_metadata_gap(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
            [3, 'http', 'www.site.com', '/path/name3.html', '?f1&f2=v2'],
        ]

        infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [2, 1, 'text/html', 1, 1, 200, 1200, 303, 456],
            [3, 1, 'text/html', 1, 1, 200, 1200, 303, 456],
        ]

        contents = [
            [1, 2, 0, 'My H1'],
            [3, 2, 0, 'My H1'],
        ]

        u = UrlDocumentGenerator(iter(patterns), infos=iter(infos), contents=iter(contents))
        for url_id, document in u:
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

        #format : link_type      follow? src_urlid       dst_urlid       or_external_url
        outlinks = [
            [1, 'a', 'follow', 2, ''],
            [1, 'a', 'link_nofollow', 3, ''],
            [1, 'a', 'follow', 4, ''],
            [1, 'a', 'config_nofollow', -1, 'http://www.youtube.com'],
            [3, 'a', 'config_nofollow', -1, 'http://www.youtube.com'],
        ]

        u = UrlDocumentGenerator(iter(patterns), outlinks=iter(outlinks))
        documents = list(u)
        document = documents[0][1]
        logger.info(document)
        self.assertEquals(document['outlinks_link_nofollow_nb'], 1)
        self.assertEquals(document['outlinks_follow_nb'], 2)
        self.assertEquals(document['outlinks_config_nofollow_nb'], 1)
        self.assertEquals(document['outlinks_follow_ids'], [2, 4])
        self.assertEquals(document['outlinks_link_nofollow_ids'], [3])

        # Check that url 2 has no outlinks
        document = documents[1][1]
        logger.info(document)
        self.assertTrue('outlinks_link_nofollow_nb' not in document)
        self.assertTrue('outlinks_follow_nb' not in document)
        self.assertTrue('outlinks_config_nofollow_nb' not in document)
        self.assertTrue('outlinks_robots_follow_nb' not in document)

        # Check that url 3 has 1 outlink
        document = documents[2][1]
        logger.info(document)
        self.assertTrue('outlinks_link_nofollow_nb' not in document)
        self.assertTrue('outlinks_follow_nb' not in document)
        self.assertEquals(document['outlinks_config_nofollow_nb'], 1)

    """
    Test outlinks with a stream starting at url 2
    """
    def test_oulinks_start_url2(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
            [3, 'http', 'www.site.com', '/path/name3.html', '?f1&f2=v2'],
        ]

        #format : link_type      follow? src_urlid       dst_urlid       or_external_url
        outlinks = [
            [2, 'a', 'follow', 3, ''],
            [2, 'a', 'link_nofollow', 4, ''],
            [2, 'a', 'follow', 5, ''],
            [2, 'a', 'config_nofollow', -1, 'http://www.youtube.com'],
            [3, 'a', 'condig_nofollow', -1, 'http://www.youtube.com'],
        ]

        u = UrlDocumentGenerator(patterns, outlinks=iter(outlinks))
        documents = list(u)

        # No link for url 1
        document = documents[0][1]
        logger.info(document)
        self.assertTrue('outlinks_link_nofollow_nb' not in document)
        self.assertTrue('outlinks_follow_nb' not in document)
        self.assertTrue('outlinks_config_nofollow_nb' not in document)
        self.assertTrue('outlinks_robots_nofollow_nb' not in document)

        # Url 2
        document = documents[1][1]
        logger.info('------')
        logger.info(document)
        logger.info('------')
        self.assertEquals(document['outlinks_link_nofollow_nb'], 1)
        self.assertEquals(document['outlinks_follow_nb'], 2)
        self.assertEquals(document['outlinks_config_nofollow_nb'], 1)
        self.assertEquals(document['outlinks_follow_ids'], [3, 5])
        self.assertEquals(document['outlinks_link_nofollow_ids'], [4])

    def test_redirect_to(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        #format : link_type      follow? src_urlid       dst_urlid       or_external_url
        outlinks = [
            [1, 'r301', True, 2, ''],
        ]

        u = UrlDocumentGenerator(iter(patterns), outlinks=iter(outlinks))
        document = u.__iter__().next()
        logger.info(document)
        self.assertEquals(document[1]['redirect_to'], {'url_id': 2, 'http_code': 301})

    def test_inlinks(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        #format : link_type      follow? dst_urlid       src_urlid
        inlinks = [
            [1, 'a', 'follow', 10],
            [1, 'a', 'link_nofollow', 11],
            [1, 'a', 'follow', 12],
        ]

        u = UrlDocumentGenerator(iter(patterns), inlinks=iter(inlinks))
        document = u.__iter__().next()[1]
        logger.info(document)
        self.assertEquals(document['inlinks_link_nofollow_nb'], 1)
        self.assertEquals(document['inlinks_follow_nb'], 2)
        self.assertEquals(document['inlinks_follow_ids'], [10, 12])
        self.assertEquals(document['inlinks_link_nofollow_ids'], [11])

    def test_redirect_from(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        #format : link_type      follow? dst_urlid       src_urlid
        inlinks = [
            [1, 'r301', True, 2],
        ]

        u = UrlDocumentGenerator(iter(patterns), inlinks=iter(inlinks))
        document = u.__iter__().next()[1]
        logger.info(document)
        self.assertEquals(document['redirect_from'], [{'url_id': 2, 'http_code': 301}])

    def test_canonical_to(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
        ]

        outlinks = [
            [1, 'canonical', True, 2, ''],
            [2, 'canonical', True, 2, ''],
        ]

        u = UrlDocumentGenerator(iter(patterns), outlinks=iter(outlinks))
        documents = list(u)
        # Url 1
        self.assertEquals(documents[0][1]['canonical_url_id'], 2)
        self.assertEquals(documents[0][1]['canonical_equals'], False)
        # Url 2
        self.assertEquals(documents[1][1]['canonical_url_id'], 2)
        self.assertEquals(documents[1][1]['canonical_equals'], True)

    def test_canonical_from(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
        ]

        inlinks = [
            [1, 'canonical', True, 5],
            [2, 'canonical', True, 17],
            [2, 'canonical', True, 20],
        ]

        u = UrlDocumentGenerator(iter(patterns), inlinks=iter(inlinks))
        documents = list(u)
        # Url 1
        self.assertEquals(documents[0][1]['canonical_nb_duplicates'], 1)
        self.assertEquals(documents[0][1]['canonical_duplicate_ids'], [5])
        # Url 2
        self.assertEquals(documents[1][1]['canonical_nb_duplicates'], 2)
        self.assertEquals(documents[1][1]['canonical_duplicate_ids'], [17, 20])
