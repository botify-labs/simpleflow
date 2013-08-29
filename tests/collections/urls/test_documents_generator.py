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
        document_expected = {
            'id': 1,
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
            'inlinks_nb': {'total': 0, 'follow_unique': 0, 'follow': 0, 'nofollow_meta': 0, 'nofollow_link': 0, 'nofollow_robots': 0},
            'inlinks': {},
            'outlinks_nb': {
                'total': 0,
                'follow_unique': 0,
                'nofollow_config': 0,
                'follow': 0,
                'nofollow_meta': 0,
                'nofollow_link': 0,
                'nofollow_robots': 0
            },
            'outlinks': {},
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

        infos = (
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [2, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [3, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        )

        #format : link_type      follow? src_urlid       dst_urlid       or_external_url
        outlinks = [
            [1, 'a', ['follow'], 2, ''],
            [1, 'a', ['nofollow_link'], 3, ''],
            [1, 'a', ['follow'], 4, ''],
            [1, 'a', ['nofollow_config'], -1, 'http://www.youtube.com'],
            [3, 'a', ['nofollow_config'], -1, 'http://www.youtube.com'],
        ]

        u = UrlDocumentGenerator(iter(patterns), outlinks=iter(outlinks), infos=iter(infos))
        documents = list(u)
        document = documents[0][1]
        logger.info(document)
        self.assertEquals(document['outlinks_nb']['nofollow_link'], 1)
        self.assertEquals(document['outlinks_nb']['follow'], 2)
        self.assertEquals(document['outlinks_nb']['nofollow_config'], 1)
        self.assertEquals(document['outlinks']['follow'], [2, 4])
        self.assertEquals(document['outlinks']['nofollow_link'], [3])

        # Check that url 2 has no outlinks
        document = documents[1][1]
        logger.info(document)
        self.assertEquals(document['outlinks_nb'],
                          {
                              'total': 0,
                              'follow_unique': 0,
                              'nofollow_config': 0,
                              'follow': 0,
                              'nofollow_meta': 0,
                              'nofollow_link': 0,
                              'nofollow_robots': 0
                          })

        # Check that url 3 has 1 outlink
        document = documents[2][1]
        logger.info(document)
        self.assertEquals(document['outlinks_nb']['nofollow_config'], 1)

    def test_outlinks_follow(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        infos = (
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        )

        #format : link_type      follow? src_urlid       dst_urlid       or_external_url
        outlinks = [
            [1, 'a', ['follow'], 2, ''],
            [1, 'a', ['nofollow_link'], 2, ''],
            [1, 'a', ['follow'], 2, ''],
            [1, 'a', ['follow'], 3, ''],
        ]

        u = UrlDocumentGenerator(iter(patterns), outlinks=iter(outlinks), infos=iter(infos))
        documents = list(u)
        document = documents[0][1]
        logger.info(document)
        self.assertEquals(document['outlinks_nb']['total'], 4)
        self.assertEquals(document['outlinks_nb']['nofollow_link'], 1)
        self.assertEquals(document['outlinks_nb']['follow'], 3)
        self.assertEquals(document['outlinks_nb']['follow_unique'], 2)
        self.assertEquals(document['outlinks']['follow'], [2, 3])
        self.assertEquals(document['outlinks']['nofollow_link'], [2])

    def test_inlinks_follow(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        infos = (
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        )

        #format : dst_url_id link_type      follow? src_urlid
        inlinks = [
            [1, 'a', ['follow'], 2],
            [1, 'a', ['follow'], 2],
            [1, 'a', ['follow'], 2],
            [1, 'a', ['nofollow_link'], 3],
            [1, 'a', ['follow'], 3],
        ]

        u = UrlDocumentGenerator(iter(patterns), inlinks=iter(inlinks), infos=iter(infos))
        documents = list(u)
        document = documents[0][1]
        logger.info(document)
        self.assertEquals(document['inlinks_nb']['total'], 5)
        self.assertEquals(document['inlinks_nb']['nofollow_link'], 1)
        self.assertEquals(document['inlinks_nb']['follow'], 4)
        self.assertEquals(document['inlinks_nb']['follow_unique'], 2)
        self.assertEquals(document['inlinks']['follow'], [2, 3])
        self.assertEquals(document['inlinks']['nofollow_link'], [3])

    """
    Test outlinks with a stream starting at url 2
    """
    def test_outlinks_start_url2(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
            [3, 'http', 'www.site.com', '/path/name3.html', '?f1&f2=v2'],
        ]

        infos = (
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [2, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [3, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        )

        #format : link_type      follow? src_urlid       dst_urlid       or_external_url
        outlinks = [
            [2, 'a', ['follow'], 3, ''],
            [2, 'a', ['nofollow_link'], 4, ''],
            [2, 'a', ['follow'], 5, ''],
            [2, 'a', ['nofollow_config'], -1, 'http://www.youtube.com'],
            [3, 'a', ['nofollow_config'], -1, 'http://www.youtube.com'],
        ]

        u = UrlDocumentGenerator(patterns, outlinks=iter(outlinks), infos=iter(infos))
        documents = list(u)

        # No link for url 1
        document = documents[0][1]
        logger.info(document)
        self.assertEquals(document['outlinks_nb'],
                          {
                              'total': 0,
                              'follow_unique': 0,
                              'nofollow_config': 0,
                              'follow': 0,
                              'nofollow_meta': 0,
                              'nofollow_link': 0,
                              'nofollow_robots': 0
                          })

        # Url 2
        document = documents[1][1]
        self.assertEquals(document['outlinks_nb']['nofollow_link'], 1)
        self.assertEquals(document['outlinks_nb']['follow'], 2)
        self.assertEquals(document['outlinks_nb']['nofollow_config'], 1)
        self.assertEquals(document['outlinks']['follow'], [3, 5])
        self.assertEquals(document['outlinks']['nofollow_link'], [4])

    def test_redirect_to(self):
        """
        * Url 1 redirect to url 2
        * Url 2 redirecto to external url which is youtube.com
        * Url 3 redirects to url 4 that is not crawled
          (usually a not crawled url is not returned by UrlDocumentGeneral except for redirects and canonicals
        """
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
            [3, 'http', 'www.site.com', '/path/name3.html', '?f1&f2=v2'],
            [4, 'http', 'www.site.com', '/path/name4.html', ''],
        ]

        infos = (
            [1, 1, 'text/html', 0, 1, 301, 1200, 303, 456],
            [2, 1, 'text/html', 0, 1, 302, 1200, 303, 456],
            [3, 1, 'text/html', 0, 1, 302, 1200, 303, 456],
            [4, 1, 'text/html', 0, 1, 0, 0, 0, 0],
        )

        #format : link_type      follow? src_urlid       dst_urlid       or_external_url
        outlinks = [
            [1, 'r301', True, 2, ''],
            [2, 'r302', True, -1, 'http://www.youtube.com'],
            [3, 'r301', True, 4, ''],
        ]

        inlinks = [
            [2, 'r301', True, 1],
            [4, 'r301', True, 3],
        ]

        u = UrlDocumentGenerator(iter(patterns),
                                 outlinks=iter(outlinks),
                                 inlinks=iter(inlinks),
                                 infos=iter(infos))
        documents = u.__iter__()

        document = documents.next()
        self.assertEquals(document[1]['redirects_to'], {'url_id': 2, 'http_code': 301})

        document = documents.next()
        self.assertEquals(document[1]['redirects_to'], {'url': 'http://www.youtube.com', 'http_code': 302})

        document = documents.next()
        self.assertEquals(document[1]['redirects_to'], {'url_id': 4, 'http_code': 301})

        document = documents.next()
        self.assertEquals(document[1], {'url': 'http://www.site.com/path/name4.html', 'http_code': 0})

    def test_inlinks(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        infos = (
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        )

        #format : link_type      follow? dst_urlid       src_urlid
        inlinks = [
            [1, 'a', ['follow'], 10],
            [1, 'a', ['nofollow_link'], 11],
            [1, 'a', ['follow'], 12],
        ]

        u = UrlDocumentGenerator(iter(patterns), inlinks=iter(inlinks), infos=iter(infos))
        document = u.__iter__().next()[1]
        logger.info(document)
        self.assertEquals(document['inlinks_nb']['nofollow_link'], 1)
        self.assertEquals(document['inlinks_nb']['follow'], 2)
        self.assertEquals(document['inlinks']['follow'], [10, 12])
        self.assertEquals(document['inlinks']['nofollow_link'], [11])

    def test_redirect_from(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        infos = (
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        )

        #format : link_type      follow? dst_urlid       src_urlid
        inlinks = [
            [1, 'r301', True, 2],
        ]

        u = UrlDocumentGenerator(iter(patterns), inlinks=iter(inlinks), infos=iter(infos))
        document = u.__iter__().next()[1]
        logger.info(document)
        self.assertEquals(document['redirects_from'], [{'url_id': 2, 'http_code': 301}])

    def test_canonical_to(self):
        """
        * Url 1 has a canonical to url 2
        * Url 2 has a canonical to the same url
        * Url 3 has a canonical to an external url
        * Url 4 has a canonical to url 5 that is not crawled (should not be stored into ES, only if it's a canonical or a redirect)
        """

        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
            [3, 'http', 'www.site.com', '/path/name3.html', ''],
            [4, 'http', 'www.site.com', '/path/name4.html', ''],
            [5, 'http', 'www.site.com', '/path/name4.html', ''],
        ]

        infos = (
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [2, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [3, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [4, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [5, 1, 'text/html', 0, 1, 0, 0, 0, 0],
        )

        outlinks = [
            [1, 'canonical', True, 2, ''],
            [2, 'canonical', True, 2, ''],
            [3, 'canonical', True, -1, 'http://www.youtube.com'],
            [4, 'canonical', True, 5, ''],
        ]

        inlinks = [
            [2, 'canonical', True, 1],
            [2, 'canonical', True, 2],
            [5, 'canonical', True, 4],

        ]

        u = UrlDocumentGenerator(iter(patterns), outlinks=iter(outlinks), infos=iter(infos), inlinks=iter(inlinks))
        documents = list(u)

        # Url 1
        self.assertEquals(documents[0][1]['canonical_url']['id'], 2)
        self.assertEquals(documents[0][1]['canonical_equals'], False)
        # Url 2
        self.assertEquals(documents[1][1]['canonical_url']['id'], 2)
        self.assertEquals(documents[1][1]['canonical_equals'], True)
        # Url 3
        self.assertEquals(documents[2][1]['canonical_url']['url'], "http://www.youtube.com")
        self.assertEquals(documents[2][1]['canonical_equals'], False)
        # Url 4
        self.assertEquals(documents[3][1]['canonical_url']['id'], 5)
        self.assertEquals(documents[3][1]['canonical_equals'], False)
        # Url 5
        self.assertEquals(documents[4][1], {"url": "http://www.site.com/path/name4.html", "http_code": 0})

    def test_canonical_from(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
        ]

        infos = (
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [2, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        )

        inlinks = [
            [1, 'canonical', True, 5],
            [2, 'canonical', True, 17],
            [2, 'canonical', True, 20],
        ]

        u = UrlDocumentGenerator(iter(patterns), inlinks=iter(inlinks), infos=iter(infos))
        documents = list(u)
        # Url 1
        self.assertEquals(documents[0][1]['canonical_from_nb'], 1)
        self.assertEquals(documents[0][1]['canonical_from'], [5])
        # Url 2
        self.assertEquals(documents[1][1]['canonical_from_nb'], 2)
        self.assertEquals(documents[1][1]['canonical_from'], [17, 20])
