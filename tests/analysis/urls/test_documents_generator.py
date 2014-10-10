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
from cdf.features.links.helpers.masks import list_to_mask
from cdf.features.main.streams import IdStreamDef, InfosStreamDef
from cdf.features.links.streams import (
    OutlinksStreamDef,
    InlinksStreamDef,
    BadLinksStreamDef,
    LinksToNonStrategicStreamDef,
    InlinksPercentilesStreamDef
)
from cdf.features.semantic_metadata.streams import (
    ContentsStreamDef,
    ContentsDuplicateStreamDef,
    ContentsCountStreamDef
)
from cdf.features.sitemaps.streams import SitemapStreamDef

logger.setLevel(logging.DEBUG)


def _next_doc(generator):
    return next(generator)[1]


class TestBasicInfoGeneration(unittest.TestCase):
    def setUp(self):
        self.ids = [
            [1, 'http', 'www.site.com', '/path/name.html', ''],
        ]
        self.infos = [
            [1, 1, '?', 0, 1, 200, 1200, 303, 456],
        ]

    def test_url_infos(self):
        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(self.ids)),
            InfosStreamDef.load_iterator(iter(self.infos))
        ])
        document = list(gen)[0]
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
            'delay_last_byte': 456,
        }

        for key, expected in document_expected.items():
            self.assertEquals(document[1][key], expected)

        self.assertFalse('delay1' in document)
        self.assertFalse('delay2' in document)

    def test_url_infos_with_lang(self):
        self.infos[0].append('fr')
        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(self.ids)),
            InfosStreamDef.load_iterator(iter(self.infos))
        ])
        document = list(gen)[0][1]
        self.assertEquals(document['lang'], 'fr')

    def test_query_string_without_value(self):
        ids = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(ids)),
            InfosStreamDef.load_iterator(iter(self.infos))
        ])

        document = _next_doc(gen)
        self.assertEquals(document['query_string'], '?f1&f2=v2')
        self.assertEquals(document['query_string_keys'], ['f1', 'f2'])

    def test_info_content_type(self):
        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(self.ids)),
            InfosStreamDef.load_iterator(iter(self.infos))
        ])
        document = _next_doc(gen)
        self.assertEquals(document['content_type'], 'not-set')

        infos = [
            [1, 1, 'text', 0, 1, 200, 1200, 303, 456],
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(self.ids)),
            InfosStreamDef.load_iterator(iter(infos))
        ])
        document = _next_doc(gen)
        self.assertEquals(document['content_type'], 'text')

    def test_query_string(self):
        ids = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1=v1&f2=v2'],
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(ids)),
            InfosStreamDef.load_iterator(iter(self.infos))
        ])
        document = _next_doc(gen)
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

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(self.ids)),
            InfosStreamDef.load_iterator(iter(infos))
        ])
        document = _next_doc(gen)
        self.assertEqual(document['gzipped'], True)
        self.assertEqual(document['metadata']['robots']['noindex'], False)
        self.assertEqual(document['metadata']['robots']['nofollow'], False)

        document = _next_doc(gen)
        self.assertEqual(document['gzipped'], True)
        self.assertEqual(document['metadata']['robots']['noindex'], True)
        self.assertEqual(document['metadata']['robots']['nofollow'], False)

        document = _next_doc(gen)
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

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(ids)),
            InfosStreamDef.load_iterator(iter(infos)),
            ContentsStreamDef.load_iterator(iter(contents)),
        ])

        document = _next_doc(gen)
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

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(self.ids)),
            InfosStreamDef.load_iterator(iter(self.infos)),
            ContentsStreamDef.load_iterator(iter(contents)),
        ])

        for url_id, document in gen:
            metadata = document['metadata']
            if document['id'] in (1, 3):
                self.assertEquals(metadata['h1']['contents'], ['My H1'])
            else:
                self.assertTrue('contents' not in metadata)

    def test_metadata_duplicate(self):
        duplicates = [
            [1, 1, 3, True, "2;3;4"],
            [2, 2, 0, True, ""],
            [3, 4, 3, False, "2;3;4"],
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(self.ids)),
            InfosStreamDef.load_iterator(iter(self.infos)),
            ContentsDuplicateStreamDef.load_iterator(iter(duplicates))
        ])

        # check for url1
        document = _next_doc(gen)
        dup = document['metadata']
        self.assertEqual(dup['title']['duplicates']['nb'], 3)
        self.assertEqual(dup['title']['duplicates']['urls'], [2, 3, 4])
        self.assertEqual(dup['title']['duplicates']['urls_exists'], True)

        # check for url2
        document = _next_doc(gen)
        dup = document['metadata']
        self.assertEqual(dup['h1']['duplicates']['nb'], 0)
        self.assertFalse('urls' in dup['h1']['duplicates'])
        self.assertFalse('urls_exists' in dup['h1']['duplicates'])

        # check for url3
        document = _next_doc(gen)
        dup = document['metadata']
        self.assertEqual(dup['description']['duplicates']['nb'], 3)
        self.assertEqual(dup['description']['duplicates']['urls'], [2, 3, 4])
        self.assertEqual(dup['description']['duplicates']['urls_exists'], True)

    def test_urlcontents_count(self):
        urlcontents_count = [
            [1, 1, 1],
            [1, 2, 1],
            [1, 4, 2],
            [2, 1, 2],
            [2, 2, 1],
            [3, 1, 1],
            [3, 3, 5],
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(self.ids)),
            InfosStreamDef.load_iterator(iter(self.infos)),
            ContentsCountStreamDef.load_iterator(iter(urlcontents_count))
        ])

        # check for url1
        document = _next_doc(gen)
        metadata = document['metadata']
        self.assertEqual(metadata['title']['nb'], 1)
        self.assertEqual(metadata['h1']['nb'], 1)
        self.assertEqual(metadata['h2']['nb'], 0)
        self.assertEqual(metadata['description']['nb'], 2)
        self.assertEqual(metadata['h3']['nb'], 0)

        # check for url2
        document = _next_doc(gen)
        metadata = document['metadata']
        self.assertEqual(metadata['title']['nb'], 2)
        self.assertEqual(metadata['h1']['nb'], 1)
        self.assertEqual(metadata['h2']['nb'], 0)
        self.assertEqual(metadata['description']['nb'], 0)
        self.assertEqual(metadata['h3']['nb'], 0)

        # check for url3
        document = _next_doc(gen)
        metadata = document['metadata']
        self.assertEqual(metadata['title']['nb'], 1)
        self.assertEqual(metadata['h1']['nb'], 0)
        self.assertEqual(metadata['h2']['nb'], 5)
        self.assertEqual(metadata['description']['nb'], 0)
        self.assertEqual(metadata['h3']['nb'], 0)


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
            [1, 'a', list_to_mask(['follow']), 2],
            [1, 'a', list_to_mask(['follow']), 2],
            [1, 'a', list_to_mask(['follow']), 2],
            [1, 'a', list_to_mask(['link']), 3],
            [1, 'a', list_to_mask(['link', 'meta']), 3],
            [1, 'a', list_to_mask(['follow']), 3],
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(patterns)),
            InfosStreamDef.load_iterator(iter(infos)),
            InlinksStreamDef.load_iterator(iter(inlinks)),
        ])

        document = _next_doc(gen)
        inlinks = document['inlinks_internal']
        self.assertEquals(inlinks['nb']['total'], 6)
        self.assertEquals(inlinks['nb']['unique'], 2)
        self.assertEquals(inlinks['nb']['nofollow']['total'], 2)
        self.assertEquals(inlinks['nb']['nofollow']['unique'], 1)
        expected_combinations = {
            "link": 1,
            "meta": 0,
            "link_meta": 1,
        }
        self.assertEquals(inlinks['nb']['nofollow']['combinations'],
                          expected_combinations)
        self.assertEquals(inlinks['nb']['follow']['total'], 4)
        self.assertEquals(inlinks['nb']['follow']['unique'], 2)
        expected_inlinks = [
            [2, list_to_mask(['follow'])],
            [3, list_to_mask(['link'])],
            [3, list_to_mask(['link', 'meta'])],
            [3, list_to_mask(['follow'])]
        ]
        self.assertEquals(inlinks['urls'], expected_inlinks)
        self.assertEquals(inlinks['urls_exists'], True)


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
            [1, 'a', list_to_mask(['follow']), 2, ''],
            [1, 'a', list_to_mask(['link']), 3, ''],
            [1, 'a', list_to_mask(['follow']), 4, ''],
            [1, 'a', list_to_mask(['follow']), 4, ''],
            [1, 'a', list_to_mask(['link']), 4, ''],
            [1, 'a', list_to_mask(['follow']), -1, 'http://www.youtube.com'],
            [1, 'a', list_to_mask(['follow']), -1, 'http://www.youtube.com'],
            [1, 'a', list_to_mask(['meta']), -1, 'http://www.youtube.com'],
            [3, 'a', list_to_mask(['follow']), -1, 'http://www.youtube.com'],
            [3, 'a', list_to_mask(['robots', 'link']), 5, ''],
            [3, 'a', list_to_mask(['robots', 'link']), 5, ''],
            [3, 'a', list_to_mask(['link']), 6, ''],
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(self.ids)),
            InfosStreamDef.load_iterator(iter(self.infos)),
            OutlinksStreamDef.load_iterator(iter(outlinks))
        ])

        # check for url1
        document = _next_doc(gen)
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
        self.assertEquals(int_outlinks_nb['total'], 5)
        self.assertEquals(int_outlinks_nb['unique'], 3)
        self.assertEquals(int_outlinks_nb['follow']['total'], 3)
        self.assertEquals(int_outlinks_nb['follow']['unique'], 2)

        self.assertEquals(ext_outlinks_nb['total'], 3)
        self.assertEquals(ext_outlinks_nb['unique'], 1)
        self.assertEquals(ext_outlinks_nb['follow']['total'], 2)
        self.assertEquals(ext_outlinks_nb['follow']['unique'], 1)
        self.assertEquals(ext_outlinks_nb['nofollow']['total'], 1)
        self.assertEquals(ext_outlinks_nb['nofollow']['unique'], 1)
        expected_outlinks_internal = [
            [2, list_to_mask(['follow'])],
            [3, list_to_mask(['link'])],
            [4, list_to_mask(['follow'])],
            [4, list_to_mask(['link'])]
        ]
        self.assertEquals(document['outlinks_internal']['urls'],
                          expected_outlinks_internal)
        self.assertEquals(document['outlinks_internal']['urls_exists'], True)

        # check for url2
        # check that url 2 has no outlinks
        document = _next_doc(gen)
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
                         {'total': 0, 'unique': 0, 'combinations': expected_combinations})

        # check for url3
        # check that url 3 has 1 outlink
        document = _next_doc(gen)
        int_outlinks_nb = document['outlinks_internal']['nb']
        ext_outlinks_nb = document['outlinks_external']['nb']
        self.assertEquals(ext_outlinks_nb['follow']['total'], 1)
        self.assertEquals(ext_outlinks_nb['follow']['unique'], 1)
        self.assertEquals(ext_outlinks_nb['nofollow']['total'], 0)
        self.assertEquals(ext_outlinks_nb['nofollow']['unique'], 0)
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
            [5, list_to_mask(["robots", "link"])],
            [6, list_to_mask(["link"])]
        ]
        self.assertEqual(document['outlinks_internal']['urls'],
                         expected_outlinks)
        self.assertEqual(document['outlinks_internal']['urls_exists'], True)

    def test_outlinks_follow(self):
        ids = self.ids[:1]
        infos = self.infos[:1]

        #format : link_type      follow? src_urlid       dst_urlid       or_external_url
        outlinks = [
            [1, 'a', list_to_mask(['follow']), 2, ''],
            [1, 'a', list_to_mask(['link']), 2, ''],
            [1, 'a', list_to_mask(['follow']), 2, ''],
            [1, 'a', list_to_mask(['follow']), 3, ''],
            # these 2 cases should be considered as internal link
            [1, 'a', list_to_mask(['robots']), -1, 'www.site.com'],
            [1, 'a', list_to_mask(['robots']), -1, 'www.site.com'],
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(ids)),
            InfosStreamDef.load_iterator(iter(infos)),
            OutlinksStreamDef.load_iterator(iter(outlinks))
        ])
        document = _next_doc(gen)

        int_outlinks_nb = document['outlinks_internal']['nb']

        self.assertEquals(int_outlinks_nb['total'], 6)
        self.assertEquals(int_outlinks_nb['nofollow']['total'], 3)
        self.assertEquals(int_outlinks_nb['nofollow']['unique'], 2)
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
            [2, list_to_mask(['follow'])],
            [2, list_to_mask(['link'])],
            [3, list_to_mask(['follow'])]
        ]
        self.assertEquals(document['outlinks_internal']['urls'],
                          expected_outlinks)
        self.assertEquals(document['outlinks_internal']['urls_exists'], True)

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

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(patterns)),
            InfosStreamDef.load_iterator(iter(infos)),
            BadLinksStreamDef.load_iterator(iter(badlinks))
        ])

        expected_1 = {
            '3xx': {
                'nb': 3,
                'urls': [100, 101, 102],
                'urls_exists': True
            },
            '4xx': {
                'nb': 1,
                'urls': [103],
                'urls_exists': True
            },
            '5xx': {
                'nb': 1,
                'urls': [5],
                'urls_exists': True
            },
            'non_strategic': {
                'nb': 0
            },
            'total': 5,
            'total_bad_http_codes': 5
        }
        expected_2 = {
            '3xx': {
                'nb': 0
            },
            '4xx': {
                'nb': 11,
                'urls': range(100, 110),
                'urls_exists': True
            },
            '5xx': {
                'nb': 0
            },
            'non_strategic': {
                'nb': 0
            },
            'total': 11,
            'total_bad_http_codes': 11
        }

        key = 'outlinks_errors'

        # check url1
        document = _next_doc(gen)
        self.assertDictEqual(document[key], expected_1)

        # check url2
        document = _next_doc(gen)
        self.assertDictEqual(document[key], expected_2)

    def test_links(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
        ]

        links_to_non_strategic_urls = [
            [1, 5],
            [1, 100],
            [1, 101],
            [1, 102],
            [1, 103]
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(patterns)),
            LinksToNonStrategicStreamDef.load_iterator(iter(links_to_non_strategic_urls))
        ])

        expected_1 = {
            '3xx': {'nb': 0},
            '4xx': {'nb': 0},
            '5xx': {'nb': 0},
            'total_bad_http_codes': 0,
            'total': 0,
            'non_strategic': {
                'nb': 5,
                'urls': [5, 100, 101, 102, 103],
                'urls_exists': True
            }
        }
        expected_2 = {
            '3xx': {'nb': 0},
            '4xx': {'nb': 0},
            '5xx': {'nb': 0},
            'total_bad_http_codes': 0,
            'total': 0,
            'non_strategic': {
                'nb': 0
            }
        }

        key = 'outlinks_errors'

        # check url1
        document = _next_doc(gen)
        self.assertDictEqual(document[key], expected_1)

        # check url2
        document = _next_doc(gen)
        self.assertDictEqual(document[key], expected_2)


    def test_inlinks_percentile_id(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
            [2, 'http', 'www.site.com', '/path/name2.html', '?f1&f2=v2'],
        ]

        percentile_id = [
            [1, 10, 4],
            [2, 5, 3]
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(patterns)),
            InlinksPercentilesStreamDef.load_iterator(iter(percentile_id))
        ])

        # check url1
        document = _next_doc(gen)
        self.assertEqual(document["inlinks_internal"]["percentile_id"], 10)

        # check url2
        document = _next_doc(gen)
        self.assertEqual(document["inlinks_internal"]["percentile_id"], 5)


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
            [1, 'r301', list_to_mask(['follow']), 2, ''],
            [2, 'r302', list_to_mask(['follow']), -1, 'http://www.youtube.com'],
            [3, 'r301', list_to_mask(['follow']), 4, ''],
        ]

        inlinks = [
            [2, 'r301', list_to_mask(['follow']), 1],
            [4, 'r301', list_to_mask(['follow']), 3],
    ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(patterns)),
            OutlinksStreamDef.load_iterator(iter(outlinks)),
            InlinksStreamDef.load_iterator(iter(inlinks)),
            InfosStreamDef.load_iterator(iter(infos))
        ])

        document = _next_doc(gen)
        redirect_to = document['redirect']['to']
        self.assertEquals(redirect_to['url'], {'url_id': 2, 'http_code': 301})
        self.assertEqual(redirect_to['url_exists'], True)

        document = _next_doc(gen)
        redirect_to = document['redirect']['to']
        self.assertEquals(redirect_to['url'],
                          {'url_str': 'http://www.youtube.com', 'http_code': 302})
        # for external url, key `url_id` should be cleaned from the document
        self.assertFalse('url_id' in redirect_to['url'])
        self.assertEqual(redirect_to['url_exists'], True)

        document = _next_doc(gen)
        redirect_to = document['redirect']['to']
        self.assertEquals(redirect_to['url'], {'url_id': 4, 'http_code': 301})
        self.assertEqual(redirect_to['url_exists'], True)

        # this is a non-crawled page but has received an incoming redirection
        # so we generate a minimal document for it
        document = _next_doc(gen)
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
            [1, 'r301', list_to_mask(['follow']), 2],
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(patterns)),
            InlinksStreamDef.load_iterator(iter(inlinks)),
            InfosStreamDef.load_iterator(iter(infos))
        ])

        document = _next_doc(gen)
        expected = {
            'urls': [
                # url_id, http_code
                [2, 301]
            ],
            'nb': 1,
            'urls_exists': True
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
            [1, 'canonical', list_to_mask(['follow']), 2, ''],
            [2, 'canonical', list_to_mask(['follow']), 2, ''],
            [3, 'canonical', list_to_mask(['follow']), -1, 'http://www.youtube.com'],
            [4, 'canonical', list_to_mask(['follow']), 5, ''],
            # Check that we take only the first canonical for url 4
            [4, 'canonical', list_to_mask(['follow']), 6, ''],
        ]

        inlinks = [
            [2, 'canonical', list_to_mask(['follow']), 1],
            [2, 'canonical', list_to_mask(['follow']), 2],
            [5, 'canonical', list_to_mask(['follow']), 4],
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(patterns)),
            InfosStreamDef.load_iterator(iter(infos)),
            InlinksStreamDef.load_iterator(iter(inlinks)),
            OutlinksStreamDef.load_iterator(iter(outlinks))
        ])

        # Url 1
        canonical_to = _next_doc(gen)['canonical']['to']
        self.assertEquals(canonical_to['url']['url_id'], 2)
        self.assertEquals(canonical_to['equal'], False)
        self.assertEquals(canonical_to['url_exists'], True)

        # Url 2
        canonical_to = _next_doc(gen)['canonical']['to']
        self.assertEquals(canonical_to['url']['url_id'], 2)
        self.assertEquals(canonical_to['equal'], True)
        self.assertEquals(canonical_to['url_exists'], True)

        # Url 3
        canonical_to = _next_doc(gen)['canonical']['to']
        self.assertEquals(canonical_to['url']['url_str'], "http://www.youtube.com")
        # for external url, key `url_id` should be cleaned from document
        self.assertFalse('url_id' in canonical_to['url'])
        self.assertEquals(canonical_to['equal'], False)
        self.assertEquals(canonical_to['url_exists'], True)

        # # Url 4
        canonical_to = _next_doc(gen)['canonical']['to']
        self.assertEquals(canonical_to['url']['url_id'], 5)
        self.assertEquals(canonical_to['equal'], False)
        self.assertEquals(canonical_to['url_exists'], True)

        # Url 5
        expected = {
            'id': 5,
            'url': 'http://www.site.com/path/name4.html',
            'http_code': 0
        }
        self.assertEquals(_next_doc(gen), expected)

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
            [1, 'canonical', list_to_mask(['follow']), 5],
            [2, 'canonical', list_to_mask(['follow']), 17],
            [2, 'canonical', list_to_mask(['follow']), 20],
            [3, 'canonical', list_to_mask(['follow']), 3],  # self canonical
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(patterns)),
            InfosStreamDef.load_iterator(iter(infos)),
            InlinksStreamDef.load_iterator(iter(inlinks))
        ])

        # Url 1
        canonical_from = _next_doc(gen)['canonical']['from']
        self.assertEquals(canonical_from['nb'], 1)
        self.assertEquals(canonical_from['urls'], [5])
        self.assertEquals(canonical_from['urls_exists'], True)

        # Url 2
        canonical_from = _next_doc(gen)['canonical']['from']
        self.assertEquals(canonical_from['nb'], 2)
        self.assertEquals(canonical_from['urls'], [17, 20])
        self.assertEquals(canonical_from['urls_exists'], True)

        # Url 3
        # should not count self canonical
        canonical_from = _next_doc(gen)['canonical']['from']
        self.assertEqual(canonical_from, {'nb': 0})
        self.assertFalse('urls_exists' in canonical_from)


class TestSitemapGeneration(unittest.TestCase):
    def test_sitemap_generation(self):
        ids = [
            [0, "http", "www.site.com", "/path/index.html", ""],
            [1, "http", "www.site.com", "/path/name.html", ""],
            [2, "http", "wwww.site.com", "/path/name.html", "?page=2"],
        ]

        sitemap_ids = [(0,), (2,), (3,)]

        id_stream = IdStreamDef.load_iterator(iter(ids))
        sitemap_stream = SitemapStreamDef.load_iterator(
            iter(sitemap_ids)
        )

        document_generator = UrlDocumentGenerator([id_stream, sitemap_stream])
        documents = list(document_generator)
        sitemap_status = [(i, d["sitemaps"]["present"]) for i, d in documents]
        expected_sitemaps_status = [(0, True), (1, False), (2, True)]
        self.assertEqual(expected_sitemaps_status, sitemap_status)


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

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(patterns)),
            InfosStreamDef.load_iterator(iter(infos)),
        ])
        self.assertRaises(StopIteration, _next_doc, gen)

    def test_total_uniques(self):
        patterns = [
            [1, 'http', 'www.site.com', '/path/name.html', '?f1&f2=v2'],
        ]

        infos = [
            [1, 1, 'text/html', 0, 1, 200, 1200, 303, 456],
        ]

        outlinks = [
            [1, 'a', 0, 2, ''],
            [1, 'a', 8, 2, ''],
            [1, 'a', 1, 2, ''],
            [1, 'a', 7, 2, ''],
            [1, 'a', 4, 2, ''],
            [1, 'a', 5, 3, ''],
            [1, 'canonical', 0, 10, ''],
        ]

        inlinks = [
            [1, 'a', 0, 2],
            [1, 'r301', 0, 3],
            [1, 'a', 2, 3],
            [1, 'a', 2, 4],
            [1, 'a', 2, 4],
            [1, 'a', 3, 4],
        ]

        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(patterns)),
            InfosStreamDef.load_iterator(iter(infos)),
            InlinksStreamDef.load_iterator(iter(inlinks)),
            OutlinksStreamDef.load_iterator(iter(outlinks)),
        ])

        document = _next_doc(gen)
        self.assertEqual(document['outlinks_internal']['nb']['unique'], 2)
        self.assertEqual(document['inlinks_internal']['nb']['unique'], 3)
        # assert that temporary data structures should be deleted
        self.assertFalse('processed_inlink_url' in document)
        self.assertFalse('processed_outlink_url' in document)
        self.assertFalse('inlinks_id_to_idx' in document)
        self.assertFalse('outlinks_id_to_idx' in document)
