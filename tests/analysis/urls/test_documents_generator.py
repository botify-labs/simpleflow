# -*- coding:utf-8 -*-
"""Unit test for url document generation
Source stream format reminder:
    - urlids: uid, protocol, host, path
    - urlinfos: uid, mask, content type, depth, date,
        http code, size, delay_first_byte, delay_last_byte
    - urllinks: src uid, link type, follow mask, dest uid
    - urlinlinks: dest uid, link type, follow mask, src uid
    - urlcontents: uid, content type code, hash.fnv64.unsigned, content
    - content_duplicate: uid, content_type, filled number,
        duplicate number, is_first, duplicating url list
"""

import unittest
import logging

from cdf.log import logger
from cdf.analysis.urls.generators.documents import UrlDocumentGenerator
from cdf.metadata.raw.masks import list_to_mask, follow_mask

logger.setLevel(logging.DEBUG)


class TestBasicInfoGeneration(unittest.TestCase):
    def setUp(self):
        self.ids = [
            [1, 'http', 'www.site.com', '/path/name.html', ''],
        ]
        self.infos = [
            [1, 1, '?', 0, 1, 200, 1200, 303, 456],
        ]

    def test_url_infos(self):
        u = UrlDocumentGenerator(iter(self.ids), infos=iter(self.infos))
        document = list(u)[0]
        document_expected = {
            'id': 1,
            'date_crawled': '2000-01-01T00:01:00',
            'url': 'http://www.site.com/path/name.html',
            'url_hash': 5539870621365162490,
            'protocol': 'http',
            'host': 'www.site.com',
            'path': '/path/name.html',
            'content_type': 'not-set',
            'gzipped': True,
            'query_string': '',
            'delay_first_byte': 303,
            'byte_size': 1200,
            'depth': 0,
            'http_code': 200,
            'delay_last_byte': 456
        }

        for key, expected in document_expected.items():
            self.assertEquals(document[1][key], expected)

        self.assertFalse('delay1' in document)
        self.assertFalse('delay2' in document)

    def test_query_string_without_value(self):
        ids = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        u = UrlDocumentGenerator(iter(ids), infos=iter(self.infos))
        document = u.__iter__().next()[1]
        self.assertEquals(document['query_string'], '?f1&f2=v2')
        self.assertEquals(document['query_string_keys'], ['f1', 'f2'])

    def test_info_content_type(self):
        u = UrlDocumentGenerator(iter(self.ids), infos=iter(self.infos))
        document = u.__iter__().next()[1]
        self.assertEquals(document['content_type'], 'not-set')

        infos = [
            [1, 1, 'text', 0, 1, 200, 1200, 303, 456],
        ]

        u = UrlDocumentGenerator(iter(self.ids), infos=iter(infos))
        document = u.__iter__().next()[1]
        self.assertEquals(document['content_type'], 'text')

    def test_query_string(self):
        ids = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1=v1&f2=v2'],
        ]

        u = UrlDocumentGenerator(iter(ids), infos=iter(self.infos))
        document = u.__iter__().next()[1]
        self.assertEquals(document['query_string'], '?f1=v1&f2=v2')
        self.assertEquals(document['query_string_keys'], ['f1', 'f2'])


class TestMetadataGeneration(unittest.TestCase):
    def setUp(self):
        self.ids = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
            [3, 'http', 'www.site.com', '/path/name3.html', '?f1&f2=v2'],
        ]
        self.infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [2, 1, 'text/html', 1, 1, 200, 1200, 303, 456],
            [3, 1, 'text/html', 1, 1, 200, 1200, 303, 456],
        ]

    def test_meta_flags(self):
        infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [2, 5, 'text/html', 1, 1, 200, 1200, 303, 456],
            [3, 8, 'text/html', 1, 1, 200, 1200, 303, 456],
        ]

        u = UrlDocumentGenerator(iter(self.ids), infos=iter(infos))
        documents = list(u)
        document = documents[0][1]
        self.assertEqual(document['gzipped'], True)
        self.assertEqual(document['metadata']['robots']['noindex'], False)
        self.assertEqual(document['metadata']['robots']['nofollow'], False)

        document = documents[1][1]
        self.assertEqual(document['gzipped'], True)
        self.assertEqual(document['metadata']['robots']['noindex'], True)
        self.assertEqual(document['metadata']['robots']['nofollow'], False)

        document = documents[2][1]
        self.assertEqual(document['gzipped'], False)
        self.assertEqual(document['metadata']['robots']['noindex'], False)
        self.assertEqual(document['metadata']['robots']['nofollow'], True)

    def test_contents(self):
        ids = self.ids[:1]
        infos = self.infos[:1]
        contents = [
            [1, 2, 0, 'My first H1'],
            [1, 2, 0, 'My second H1'],
            [1, 1, 0, 'My title']
        ]

        u = UrlDocumentGenerator(iter(ids), infos=iter(infos),
                                 contents=iter(contents))
        document = list(u)[0][1]
        metadata = document['metadata']
        self.assertEquals(metadata['h1']['contents'],
                          ['My first H1', 'My second H1'])
        self.assertEquals(metadata['title']['contents'],
                          ['My title'])

    def test_contents_gap(self):
        """Test that empty metadata contents are correctly skipped"""
        contents = [
            [1, 2, 0, 'My H1'],
            # no contents for url 2
            [3, 2, 0, 'My H1'],
        ]

        u = UrlDocumentGenerator(iter(self.ids), infos=iter(self.infos),
                                 contents=iter(contents))
        for url_id, document in u:
            metadata = document['metadata']
            if document['id'] in (1, 3):
                self.assertEquals(metadata['h1']['contents'], ['My H1'])
            else:
                self.assertTrue('contents' not in metadata)

    def test_metadata_duplicate(self):
        duplicates = [
            [1, 1, 10, 3, True, [2, 3, 4]],
            [2, 2, 1, 0, True, []],
            [3, 4, 10, 3, False, [2, 3, 4]],
        ]
        u = UrlDocumentGenerator(iter(self.ids), infos=iter(self.infos),
                                 contents_duplicate=iter(duplicates))

        documents = list(u)
        # check for url1
        document = documents[0][1]
        dup = document['metadata']
        self.assertEqual(dup['title']['nb'], 10)
        self.assertEqual(dup['title']['duplicates']['nb'], 3)
        self.assertEqual(dup['title']['duplicates']['urls'], [2, 3, 4])

        # check for url2
        document = documents[1][1]
        dup = document['metadata']
        self.assertEqual(dup['h1']['nb'], 1)
        self.assertEqual(dup['h1']['duplicates']['nb'], 0)
        self.assertFalse('urls' in dup['h1']['duplicates'])

        # check for url3
        document = documents[2][1]
        dup = document['metadata']
        self.assertEqual(dup['description']['nb'], 10)
        self.assertEqual(dup['description']['duplicates']['nb'], 3)
        self.assertEqual(dup['description']['duplicates']['urls'], [2, 3, 4])


class TestInlinksGeneration(unittest.TestCase):
    def test_inlinks(self):
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
            [1, 'a', ['link_meta'], 3],
        ]

        u = UrlDocumentGenerator(iter(patterns), inlinks=iter(inlinks),
                                 infos=iter(infos))
        documents = list(u)
        document = documents[0][1]
        inlinks = document['inlinks_internal']
        self.assertEquals(inlinks['nb']['total'], 5)
        self.assertEquals(inlinks['nb']['nofollow']['total'], 2)
        expected_combinations = {
            "link": 1,
            "meta": 0,
            "link_meta": 1,
        }
        self.assertEquals(inlinks['nb']['nofollow']['combinations'],
                          expected_combinations)
        self.assertEquals(inlinks['nb']['follow']['total'], 3)
        self.assertEquals(inlinks['nb']['follow']['unique'], 1)
        expected_inlinks = [
            [2, list_to_mask(['follow']), 3],
            [3, list_to_mask(['link']), 1],
            [3, list_to_mask(['link_meta']), 1]
        ]
        self.assertEquals(inlinks['urls'], expected_inlinks)


class TestOutlinksGeneration(unittest.TestCase):
    def setUp(self):
        self.ids = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
            [3, 'http', 'www.site.com', '/path/name3.html', '?f1&f2=v2'],
        ]

        self.infos = (
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [2, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [3, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        )

    def test_outlinks(self):
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

        u = UrlDocumentGenerator(iter(self.ids), outlinks=iter(outlinks),
                                 infos=iter(self.infos))
        documents = list(u)

        # check for url1
        document = documents[0][1]
        # assert nofollow combinations
        expected_combinations = {
            "link": 2,
            "meta": 0,
            "robots": 0,
            "link_meta": 0,
            "meta_robots": 0,
            "link_robots": 0,
            "link_meta_robots": 0,
        }

        int_outlinks_nb = document['outlinks_internal']['nb']
        ext_outlinks_nb = document['outlinks_external']['nb']

        self.assertItemsEqual(int_outlinks_nb['nofollow']['combinations'],
                              expected_combinations)
        self.assertEquals(int_outlinks_nb['follow']['total'], 3)
        self.assertEquals(int_outlinks_nb['follow']['unique'], 2)
        self.assertEquals(ext_outlinks_nb['follow']['total'], 2)
        expected_outlinks_internal = [
            [2, list_to_mask(['follow']), 1],
            [3, list_to_mask(['link']), 1],
            [4, list_to_mask(['follow']), 2],
            [4, list_to_mask(['link']), 1]
        ]
        self.assertEquals(document['outlinks_internal']['urls']['all'],
                          expected_outlinks_internal)

        # check for url2
        # check that url 2 has no outlinks
        document = documents[1][1]
        int_outlinks_nb = document['outlinks_internal']['nb']
        ext_outlinks_nb = document['outlinks_external']['nb']
        self.assertEqual(int_outlinks_nb['total'], 0)
        self.assertEqual(int_outlinks_nb['follow'],
                         {'total': 0, 'unique': 0})
        expected_combinations = {
            "link": 0,
            "meta": 0,
            "robots": 0,
            "link_meta": 0,
            "meta_robots": 0,
            "link_robots": 0,
            "link_meta_robots": 0,
        }
        self.assertEqual(int_outlinks_nb['nofollow'],
                         {'total': 0, 'combinations': expected_combinations})

        # check for url3
        # check that url 3 has 1 outlink
        document = documents[2][1]
        int_outlinks_nb = document['outlinks_internal']['nb']
        ext_outlinks_nb = document['outlinks_external']['nb']
        self.assertEquals(ext_outlinks_nb['follow']['total'], 1)
        expected_combinations = {
            "link": 1,
            "meta": 0,
            "robots": 0,
            "link_meta": 0,
            "meta_robots": 0,
            "link_robots": 2,
            "link_meta_robots": 0,
        }

        self.assertEquals(int_outlinks_nb['nofollow']['total'], 3)
        self.assertEquals(int_outlinks_nb['nofollow']['combinations'],
                          expected_combinations)
        expected_outlinks = [
            [5, list_to_mask(["robots", "link"]), 2],
            [6, list_to_mask(["link"]), 1]
        ]
        self.assertItemsEqual(document['outlinks_internal']['urls']['all'],
                              expected_outlinks)
    def test_outlinks_follow(self):
        ids = self.ids[:1]
        infos = self.infos[:1]

        #format : link_type      follow? src_urlid       dst_urlid       or_external_url
        outlinks = [
            [1, 'a', ['follow'], 2, ''],
            [1, 'a', ['link'], 2, ''],
            [1, 'a', ['follow'], 2, ''],
            [1, 'a', ['follow'], 3, ''],
            # TODO ask about this 1) nofollow_combination 2) outlink list
            # these 2 cases should be considered as internal link
            [1, 'a', ['robots'], -1, 'www.site.com'],
            [1, 'a', ['robots'], -1, 'www.site.com/abc'],
        ]

        u = UrlDocumentGenerator(iter(ids), outlinks=iter(outlinks),
                                 infos=iter(infos))
        documents = list(u)
        document = documents[0][1]

        int_outlinks_nb = document['outlinks_internal']['nb']

        self.assertEquals(int_outlinks_nb['total'], 6)
        self.assertEquals(int_outlinks_nb['nofollow']['total'], 3)
        expected_combinations = {
            "link": 1,
            "meta": 0,
            "robots": 2,
            "link_meta": 0,
            "meta_robots": 0,
            "link_robots": 0,
            "link_meta_robots": 0,
        }
        self.assertEquals(int_outlinks_nb['nofollow']['combinations'],
                          expected_combinations)
        self.assertEquals(int_outlinks_nb['follow']['total'], 3)
        self.assertEquals(int_outlinks_nb['follow']['unique'], 2)
        expected_outlinks = [
            [2, list_to_mask(['follow']), 2],
            [2, list_to_mask(['link']), 1],
            [3, list_to_mask(['follow']), 1]
        ]
        self.assertEquals(document['outlinks_internal']['urls']['all'],
                          expected_outlinks)

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

        expected_nb_1 = {
            '3xx': 3,
            '4xx': 1,
            '5xx': 1,
            'total': 5,
        }
        expected_urls_1 = {
            '3xx': [100, 101, 102],
            '4xx': [103],
            '5xx': [5],
        }

        expected_2 = {
            '4xx': {
                'nb': 11,
                'urls': range(100, 110)
            },
            '3xx': {
                'nb': 0
            },
            '5xx': {
                'nb': 0
            },
            'any': {
                'nb': 11
            }
        }
        expected_nb_2 = {
            '3xx': 0,
            '4xx': 11,
            '5xx': 0,
            'total': 11,
        }
        expected_urls_2 = {
            '4xx': range(100, 110),
        }

        document = documents[0][1]
        outlinks = document['outlinks_internal']
        self.assertEqual(outlinks['nb']['errors'], expected_nb_1)

        self.assertDictContainsSubset(expected_urls_1, outlinks['urls'])

        document = documents[1][1]
        outlinks = document['outlinks_internal']
        self.assertEqual(outlinks['nb']['errors'], expected_nb_2)
        self.assertDictContainsSubset(expected_urls_2, outlinks['urls'])


class TestRedirectsGeneration(unittest.TestCase):
    def test_redirect_to(self):
        """Test for redirect_to related counters

        - Url 1 redirect to url 2
        - Url 2 redirecto to external url which is youtube.com
        - Url 3 redirects to url 4 that is not crawled
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

        document = documents.next()[1]
        redirect_to = document['redirect']['to']
        self.assertEquals(redirect_to, {'url_id': 2, 'http_code': 301})

        document = documents.next()[1]
        redirect_to = document['redirect']['to']
        self.assertEquals(redirect_to,
                          {'url': 'http://www.youtube.com', 'http_code': 302,
                           # 0 here
                           'url_id': 0})

        document = documents.next()[1]
        redirect_to = document['redirect']['to']
        self.assertEquals(redirect_to, {'url_id': 4, 'http_code': 301})

        # this is a non-crawled page but has received an incoming redirection
        # so we generate a minimal document for it
        document = documents.next()[1]
        expected = {
            'url': 'http://www.site.com/path/name4.html',
            'id': 4,
            'http_code': 0
        }
        self.assertEqual(document, expected)

    def test_redirect_from(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        ]

        #format : link_type      follow? dst_urlid       src_urlid
        inlinks = [
            [1, 'r301', ['follow'], 2],
        ]

        u = UrlDocumentGenerator(iter(patterns), inlinks=iter(inlinks),
                                 infos=iter(infos))
        document = iter(u).next()[1]
        expected = {
            'urls': [
                # url_id, http_code
                [2, 301]
            ],
            'nb': 1
        }
        self.assertEquals(document['redirect']['from'], expected)


class TestCanonicalGeneration(unittest.TestCase):
    def test_canonical_to(self):
        """Test for canonical related counters

        - Url 1 has a canonical to url 2
        - Url 2 has a canonical to the same url
        - Url 3 has a canonical to an external url
        - Url 4 has a canonical to url 5 that is not crawled
        """

        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
            [3, 'http', 'www.site.com', '/path/name3.html', ''],
            [4, 'http', 'www.site.com', '/path/name4.html', ''],
            [5, 'http', 'www.site.com', '/path/name4.html', ''],
        ]

        infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [2, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [3, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [4, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
            [5, 1, 'text/html', 0, 1, 0, 0, 0, 0],
        ]

        outlinks = [
            [1, 'canonical', ['follow'], 2, ''],
            [2, 'canonical', ['follow'], 2, ''],
            [3, 'canonical', ['follow'], -1, 'http://www.youtube.com'],
            [4, 'canonical', ['follow'], 5, ''],
            # Check that we take only the first canonical for url 4
            [4, 'canonical', ['follow'], 6, ''],
        ]

        inlinks = [
            [2, 'canonical', ['follow'], 1],
            [2, 'canonical', ['follow'], 2],
            [5, 'canonical', ['follow'], 4],
        ]

        u = UrlDocumentGenerator(iter(patterns), outlinks=iter(outlinks),
                                 infos=iter(infos), inlinks=iter(inlinks))
        documents = list(u)

        # Url 1
        canonical_to = documents[0][1]['canonical']['to']
        self.assertEquals(canonical_to['url_id'], 2)
        self.assertEquals(canonical_to['equal'], False)

        # Url 2
        canonical_to = documents[1][1]['canonical']['to']
        self.assertEquals(canonical_to['url_id'], 2)
        self.assertEquals(canonical_to['equal'], True)

        # Url 3
        canonical_to = documents[2][1]['canonical']['to']
        self.assertEquals(canonical_to['url'], "http://www.youtube.com")
        # 0 here, should clean ???
        self.assertEquals(canonical_to['url_id'], 0)
        self.assertEquals(canonical_to['equal'], False)

        # # Url 4
        canonical_to = documents[3][1]['canonical']['to']
        self.assertEquals(canonical_to['url_id'], 5)
        self.assertEquals(canonical_to['equal'], False)

        # Url 5
        expected = {
            'id': 5,
            'url': 'http://www.site.com/path/name4.html',
            'http_code': 0
        }
        self.assertEquals(documents[4][1], expected)

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

        u = UrlDocumentGenerator(iter(patterns), inlinks=iter(inlinks),
                                 infos=iter(infos))
        documents = list(u)

        # Url 1
        canonical_from = documents[0][1]['canonical']['from']
        self.assertEquals(canonical_from['nb'], 1)
        self.assertEquals(canonical_from['urls'], [5])

        # Url 2
        canonical_from = documents[1][1]['canonical']['from']
        self.assertEquals(canonical_from['nb'], 2)
        self.assertEquals(canonical_from['urls'], [17, 20])

        # Url 3
        # should not count self canonical
        self.assertEqual(documents[2][1]['canonical']['from'],
                         {'nb': 0})


class TestGlobalDocumentGeneration(unittest.TestCase):
    def test_non_crawl_url(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', ''],
            [2, 'http', 'www.site.com', '/path/name.html', ''],
            [3, 'http', 'www.site.com', '/path/name.html', ''],
        ]

        infos = [
            # http code is 0, 1, 2 respectively
            # they should generate no result
            [1, 1, 'text/html', 0, 1, 1, 1200, 303, 456],
            [2, 1, 'text/html', 0, 1, 2, 1200, 303, 456],
            [3, 1, 'text/html', 0, 1, 0, 1200, 303, 456],
        ]

        u = UrlDocumentGenerator(iter(patterns), infos=iter(infos))
        document = list(u)
        self.assertEqual(document, [])

    # TODO nofollow, noindex
    # TODO make query test independent from url ES mapping
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
            [1, 'a', follow_mask(2), 4],
            [1, 'a', follow_mask(2), 4],
            [1, 'a', follow_mask(3), 4],
        ]

        u = UrlDocumentGenerator(iter(patterns),
                                 infos=iter(infos),
                                 outlinks=iter(outlinks),
                                 inlinks=iter(inlinks))
        documents = list(u)

        document = documents[0][1]
        self.assertEqual(document['outlinks_internal']['nb']['unique'], 2)
        self.assertEqual(document['inlinks_internal']['nb']['unique'], 3)
        # assert that temporary data structures should be deleted
        self.assertFalse('processed_inlink_url' in document)
        self.assertFalse('processed_outlink_url' in document)
        self.assertFalse('inlinks_id_to_idx' in document)
        self.assertFalse('outlinks_id_to_idx' in document)
