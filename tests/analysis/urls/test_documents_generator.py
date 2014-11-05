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
