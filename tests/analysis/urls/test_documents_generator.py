# -*- coding:utf-8 -*-
import unittest
import logging

from cdf.log import logger
from cdf.analysis.urls.generators.documents import UrlDocumentGenerator
from cdf.metadata.raw.masks import list_to_mask, follow_mask

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
            'metadata_nb': {'description': 0, 'h1': 0, 'h2': 0, 'h3': 0, 'title': 0},
            'meta_noindex': False,
            'meta_nofollow': False,
            'inlinks_internal_nb': {
                'total': 0,
                'total_unique': 0,
                'follow_unique': 0,
                'follow': 0,
                'nofollow': 0,
                'nofollow_combinations': []
            },
            'inlinks_internal': [],
            'outlinks_internal_nb': {
                'total': 0,
                'total_unique': 0,
                'follow_unique': 0,
                'follow': 0,
                'nofollow': 0,
                'nofollow_combinations': []
            },
            'outlinks_external_nb': {
                'total': 0,
                'follow': 0,
                'nofollow': 0,
                'nofollow_combinations': []
            },
            'outlinks_internal': [],
        }

        for key, expected in document_expected.items():
            self.assertEquals(document[1][key], expected)

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

    def test_info_content_type(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1=v1&f2=v2'],
        ]

        infos = [
            [1, 1, '?', 0, 1, 200, 1200, 303, 456],
        ]

        u = UrlDocumentGenerator(iter(patterns), infos=iter(infos))
        document = u.__iter__().next()[1]
        self.assertEquals(document['content_type'], 'not-set')

        infos = [
            [1, 1, 'text', 0, 1, 200, 1200, 303, 456],
        ]

        u = UrlDocumentGenerator(iter(patterns), infos=iter(infos))
        document = u.__iter__().next()[1]
        self.assertEquals(document['content_type'], 'text')

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
            [1, 'a', ['link'], 3, ''],
            [1, 'a', ['follow'], 4, ''],
            [1, 'a', ['follow'], 4, ''],
            [1, 'a', ['link'], 4, ''],
            [1, 'a', ['follow'], -1, 'http://www.youtube.com'],
            [1, 'a', ['follow'], -1, 'http://www.youtube.com'],
            [3, 'a', ['follow'], -1, 'http://www.youtube.com'],
            [3, 'a', ['robots', 'link'], 5, ''],
            [3, 'a', ['robots', 'link'], 5, ''],
            [3, 'a', ['link'], 6, ''],
        ]

        u = UrlDocumentGenerator(iter(patterns), outlinks=iter(outlinks), infos=iter(infos))
        documents = list(u)
        document = documents[0][1]
        logger.info(document)
        expected_combinations = [
            {
                "key": ["link"],
                "value": 2
            },
        ]

        self.assertItemsEqual(document['outlinks_internal_nb']['nofollow_combinations'], expected_combinations)
        self.assertEquals(document['outlinks_internal_nb']['follow'], 3)
        self.assertEquals(document['outlinks_internal_nb']['follow_unique'], 2)
        self.assertEquals(document['outlinks_external_nb']['follow'], 2)
        expected_outlinks_internal = [
            [2, list_to_mask(['follow']), 1],
            [3, list_to_mask(['link']), 1],
            [4, list_to_mask(['follow']), 2],
            [4, list_to_mask(['link']), 1]
        ]
        self.assertEquals(document['outlinks_internal'], expected_outlinks_internal)

        # Check that url 2 has no outlinks
        document = documents[1][1]
        logger.info(document)
        self.assertEquals(
            document['outlinks_internal_nb'],
            {'nofollow_combinations': [], 'follow': 0, 'total': 0, 'total_unique': 0, 'nofollow': 0, 'follow_unique': 0}
        )
        # Check that url 3 has 1 outlink
        document = documents[2][1]
        logger.info(document)
        self.assertEquals(document['outlinks_external_nb']['follow'], 1)
        expected_combinations = [
            {"key": ["robots", "link"],
             "value": 2},
            {"key": ["link"],
             "value": 1}
        ]
        self.assertEquals(document['outlinks_internal_nb']['nofollow'], 3)
        self.assertEquals(document['outlinks_internal_nb']['nofollow_combinations'], expected_combinations)
        expected_outlinks = [
            [5, list_to_mask(["robots", "link"]), 2],
            [6, list_to_mask(["link"]), 1]
        ]
        self.assertItemsEqual(document['outlinks_internal'], expected_outlinks)

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
            [1, 'a', ['link'], 2, ''],
            [1, 'a', ['follow'], 2, ''],
            [1, 'a', ['follow'], 3, ''],
        ]

        u = UrlDocumentGenerator(iter(patterns), outlinks=iter(outlinks), infos=iter(infos))
        documents = list(u)
        document = documents[0][1]
        logger.info(document)
        self.assertEquals(document['outlinks_internal_nb']['total'], 4)
        self.assertEquals(document['outlinks_internal_nb']['nofollow'], 1)
        self.assertEquals(document['outlinks_internal_nb']['nofollow_combinations'], [{"key": ["link"], "value": 1}])
        self.assertEquals(document['outlinks_internal_nb']['follow'], 3)
        self.assertEquals(document['outlinks_internal_nb']['follow_unique'], 2)
        self.assertEquals(document['outlinks_internal'], [[2, list_to_mask(['follow']), 2], [2, list_to_mask(['link']), 1], [3, list_to_mask(['follow']), 1]])

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
            [1, 'a', ['link'], 3],
            [1, 'a', ['follow'], 3],
        ]

        u = UrlDocumentGenerator(iter(patterns), inlinks=iter(inlinks), infos=iter(infos))
        documents = list(u)
        document = documents[0][1]
        logger.info(document)
        self.assertEquals(document['inlinks_internal_nb']['total'], 5)
        self.assertEquals(document['inlinks_internal_nb']['nofollow'], 1)
        self.assertEquals(document['inlinks_internal_nb']['nofollow_combinations'], [{"key": ["link"], "value": 1}])
        self.assertEquals(document['inlinks_internal_nb']['follow'], 4)
        self.assertEquals(document['inlinks_internal_nb']['follow_unique'], 2)
        self.assertEquals(document['inlinks_internal'], [[2, list_to_mask(['follow']), 3], [3, list_to_mask(['link']), 1], [3, list_to_mask(['follow']), 1]])

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
            [2, 'a', ['link'], 4, ''],
            [2, 'a', ['follow'], 5, ''],
            [2, 'a', ['follow'], -1, 'http://www.youtube.com'],
            [3, 'a', ['follow'], -1, 'http://www.youtube.com'],
        ]

        u = UrlDocumentGenerator(patterns, outlinks=iter(outlinks), infos=iter(infos))
        documents = list(u)

        # No link for url 1
        document = documents[0][1]
        logger.info(document)
        self.assertEquals(document['outlinks_internal_nb'],
                          {
                              'total': 0,
                              'total_unique': 0,
                              'follow_unique': 0,
                              'nofollow': 0,
                              'follow': 0,
                              'nofollow_combinations': [],
                          })

        # Url 2
        document = documents[1][1]
        self.assertEquals(document['outlinks_internal_nb']['nofollow'], 1)
        self.assertEquals(document['outlinks_internal_nb']['nofollow_combinations'], [{"key": ["link"], "value": 1}])
        self.assertEquals(document['outlinks_internal_nb']['follow'], 2)
        self.assertEquals(document['outlinks_internal_nb']['follow_unique'], 2)
        self.assertEquals(document['outlinks_external_nb']['follow'], 1)
        self.assertEquals(document['outlinks_internal'], [[3, list_to_mask(['follow']), 1], [4, list_to_mask(['link']), 1], [5, list_to_mask(['follow']), 1]])

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
            [1, 'r301', ['follow'], 2, ''],
            [2, 'r302', ['follow'], -1, 'http://www.youtube.com'],
            [3, 'r301', ['follow'], 4, ''],
        ]

        inlinks = [
            [2, 'r301', ['follow'], 1],
            [4, 'r301', ['follow'], 3],
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
        self.assertEquals(document[1], {'url': 'http://www.site.com/path/name4.html', 'http_code': 0, 'id': 4})

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
            [1, 'a', ['link'], 11],
            [1, 'a', ['follow'], 12],
        ]

        u = UrlDocumentGenerator(iter(patterns), inlinks=iter(inlinks), infos=iter(infos))
        document = u.__iter__().next()[1]
        logger.info(document)
        self.assertEquals(document['inlinks_internal_nb']['nofollow'], 1)
        self.assertEquals(document['inlinks_internal_nb']['follow'], 2)
        self.assertEquals(document['inlinks_internal_nb']['follow_unique'], 2)
        self.assertEquals(document['inlinks_internal'], [[10, list_to_mask(['follow']), 1], [11, list_to_mask(['link']), 1], [12, list_to_mask(['follow']), 1]])

    def test_redirect_from(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        infos = (
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        )

        #format : link_type      follow? dst_urlid       src_urlid
        inlinks = [
            [1, 'r301', ['follow'], 2],
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
            [1, 'canonical', ['follow'], 2, ''],
            [2, 'canonical', ['follow'], 2, ''],
            [3, 'canonical', ['follow'], -1, 'http://www.youtube.com'],
            [4, 'canonical', ['follow'], 5, ''],
            [4, 'canonical', ['follow'], 6, ''], # Check that we take only the first canonical for url 4
        ]

        inlinks = [
            [2, 'canonical', ['follow'], 1],
            [2, 'canonical', ['follow'], 2],
            [5, 'canonical', ['follow'], 4],
        ]

        u = UrlDocumentGenerator(iter(patterns), outlinks=iter(outlinks), infos=iter(infos), inlinks=iter(inlinks))
        documents = list(u)

        # Url 1
        self.assertEquals(documents[0][1]['canonical_to']['url_id'], 2)
        self.assertEquals(documents[0][1]['canonical_to_equal'], False)
        # Url 2
        self.assertEquals(documents[1][1]['canonical_to']['url_id'], 2)
        self.assertEquals(documents[1][1]['canonical_to_equal'], True)
        # Url 3
        self.assertEquals(documents[2][1]['canonical_to']['url'], "http://www.youtube.com")
        self.assertEquals(documents[2][1]['canonical_to_equal'], False)
        # Url 4
        self.assertEquals(documents[3][1]['canonical_to']['url_id'], 5)
        self.assertEquals(documents[3][1]['canonical_to_equal'], False)
        # Url 5
        self.assertEquals(documents[4][1], {"id": 5, "url": "http://www.site.com/path/name4.html", "http_code": 0})

    def test_canonical_from(self):
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

        inlinks = [
            [1, 'canonical', ['follow'], 5],
            [2, 'canonical', ['follow'], 17],
            [2, 'canonical', ['follow'], 20],
            [3, 'canonical', ['follow'], 3],  # self canonical
        ]

        u = UrlDocumentGenerator(iter(patterns), inlinks=iter(inlinks), infos=iter(infos))
        documents = list(u)
        # Url 1
        self.assertEquals(documents[0][1]['canonical_from_nb'], 1)
        self.assertEquals(documents[0][1]['canonical_from'], [5])
        # Url 2
        self.assertEquals(documents[1][1]['canonical_from_nb'], 2)
        self.assertEquals(documents[1][1]['canonical_from'], [17, 20])
        # Url 3
        # should not count self canonical
        self.assertEqual(documents[2][1]['canonical_from_nb'], 0)

    def test_bad_links(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
        ]

        infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [2, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        ]

        badlinks = [
            [1, 5, 500],
            [1, 100, 302],
            [1, 101, 302],
            [1, 102, 302],
            [1, 103, 402],
            [2, 100, 402],
            [2, 101, 402],
            [2, 102, 402],
            [2, 103, 402],
            [2, 104, 402],
            [2, 105, 402],
            [2, 106, 402],
            [2, 107, 402],
            [2, 108, 402],
            [2, 109, 402],
            [2, 110, 402],
        ]

        u = UrlDocumentGenerator(iter(patterns),
                                 infos=iter(infos),
                                 badlinks=iter(badlinks))
        documents = list(u)

        expected_1 = {
            '3xx': {
                'nb': 3,
                'urls': [100, 101, 102]
            },
            '4xx': {
                'nb': 1,
                'urls': [103]
            },
            '5xx': {
                'nb': 1,
                'urls': [5]
            },
            'any': {
                'nb': 5
            }
        }
        expected_2 = {
            '4xx': {
                'nb': 11,
                'urls': range(100, 110)
            },
            'any': {
                'nb': 11
            }
        }

        key = 'error_links'
        self.assertDictEqual(documents[0][1][key], expected_1)
        self.assertDictEqual(documents[1][1][key], expected_2)

    def test_total_uniques(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        ]

        outlinks = [
            [1, 'a', follow_mask(0), 2, ''],
            [1, 'a', follow_mask(8), 2, ''],
            [1, 'a', follow_mask(1), 2, ''],
            [1, 'a', follow_mask(7), 2, ''],
            [1, 'a', follow_mask(4), 2, ''],
            [1, 'a', follow_mask(5), 3, ''],
            [1, 'canonical', follow_mask(0), 10, ''],
        ]

        inlinks = [
            [1, 'a', follow_mask(0), 2],
            [1, 'r301', follow_mask(0), 3],
            [1, 'a', follow_mask(2), 3],
            [1, 'a', follow_mask(4), 4],
            [1, 'a', follow_mask(6), 4],
            [1, 'a', follow_mask(7), 4],
        ]

        u = UrlDocumentGenerator(iter(patterns),
                                 infos=iter(infos),
                                 outlinks=iter(outlinks),
                                 inlinks=iter(inlinks))
        documents = list(u)

        target = documents[0][1]
        self.assertEqual(target['outlinks_internal_nb']['total_unique'], 2)
        self.assertEqual(target['inlinks_internal_nb']['total_unique'], 3)
        self.assertFalse('processed_inlink_url' in documents[0][1])
        self.assertFalse('processed_outlink_url' in documents[0][1])
