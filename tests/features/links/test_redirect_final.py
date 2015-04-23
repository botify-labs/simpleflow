from cdf.features.main.streams import IdStreamDef
from cdf.tasks.documents import UrlDocumentGenerator

__author__ = 'zeb'

import unittest

from cdf.features.links.redirect_final import (
    FinalRedirectionStreamDef,
    RedirectFinal,
    compute_final_redirects,
)


_ids1 = [
    [1, 'http', 'www.site.com', '/path/name.html', ''],
    [2, 'http', 'www.site.com', '/path/other_name.html', ''],
    [3, 'http', 'www.site.com', '/path/real_page.html', ''],
    [4, 'http', 'www.site.com', '/', ''],
]

_stream1 = [
    (1, -1, 1, 'http://www.google.com/', False, 0),
    (2, 4, 2, '', False, 200),
    (3, 4, 1, '', False, 200),
]


class TestFinalRedirectionStreamDef(unittest.TestCase):
    def test_1(self):
        gen = UrlDocumentGenerator(
            [IdStreamDef.load_iterator(_ids1), FinalRedirectionStreamDef.load_iterator(_stream1)])
        documents = list(gen)
        self.assertEqual(len(documents), 4)
        doc = documents[0][1]
        self.assertTrue("redirect" in doc)
        self.assertTrue("to" in doc["redirect"])
        redirects_to = doc['redirect']['to']
        self.assertTrue("final_url" in redirects_to)
        self.assertTrue("final_url_exists" in redirects_to)
        self.assertTrue(redirects_to["final_url_exists"])
        final_url = redirects_to["final_url"]
        self.assertTrue("url_str" in final_url)
        self.assertFalse("url_id" in final_url)
        self.assertFalse("http_code" in final_url)
        self.assertEqual("http://www.google.com/", final_url["url_str"])
        self.assertTrue("nb_hops" in redirects_to)
        self.assertEqual(1, redirects_to["nb_hops"])
        self.assertTrue("in_loop" in redirects_to)
        self.assertEqual(False, redirects_to["in_loop"])

    def test_2(self):
        gen = UrlDocumentGenerator(
            [IdStreamDef.load_iterator(_ids1), FinalRedirectionStreamDef.load_iterator(_stream1)])
        documents = list(gen)
        self.assertEqual(len(documents), 4)
        doc = documents[1][1]
        self.assertTrue("redirect" in doc)
        self.assertTrue("to" in doc["redirect"])
        redirects_to = doc['redirect']['to']
        self.assertTrue("final_url" in redirects_to)
        self.assertTrue("final_url_exists" in redirects_to)
        self.assertTrue(redirects_to["final_url_exists"])
        final_url = redirects_to["final_url"]
        self.assertFalse("url_str" in final_url)
        self.assertTrue("url_id" in final_url)
        self.assertTrue("http_code" in final_url)
        self.assertEqual(4, final_url["url_id"])
        self.assertEqual(200, final_url["http_code"])
        self.assertTrue("nb_hops" in redirects_to)
        self.assertEqual(2, redirects_to["nb_hops"])
        self.assertTrue("in_loop" in redirects_to)
        self.assertEqual(False, redirects_to["in_loop"])

    def test_3(self):
        gen = UrlDocumentGenerator(
            [IdStreamDef.load_iterator(_ids1), FinalRedirectionStreamDef.load_iterator(_stream1)])
        documents = list(gen)
        self.assertEqual(len(documents), 4)
        doc = documents[2][1]
        self.assertTrue("redirect" in doc)
        self.assertTrue("to" in doc["redirect"])
        redirects_to = doc['redirect']['to']
        self.assertTrue("final_url" in redirects_to)
        self.assertTrue("final_url_exists" in redirects_to)
        self.assertTrue(redirects_to["final_url_exists"])
        final_url = redirects_to["final_url"]
        self.assertFalse("url_str" in final_url)
        self.assertTrue("url_id" in final_url)
        self.assertTrue("http_code" in final_url)
        self.assertEqual(4, final_url["url_id"])
        self.assertEqual(200, final_url["http_code"])
        self.assertTrue("nb_hops" in redirects_to)
        self.assertEqual(1, redirects_to["nb_hops"])
        self.assertTrue("in_loop" in redirects_to)
        self.assertEqual(False, redirects_to["in_loop"])

    def test_4(self):
        gen = UrlDocumentGenerator(
            [IdStreamDef.load_iterator(_ids1), FinalRedirectionStreamDef.load_iterator(_stream1)])
        documents = list(gen)
        self.assertEqual(len(documents), 4)
        doc = documents[3][1]
        self.assertTrue("redirect" in doc)
        self.assertTrue("to" in doc["redirect"])
        redirects_to = doc['redirect']['to']
        self.assertFalse("final_url" in redirects_to)
        self.assertFalse("final_url_exists" in redirects_to)


class TestComputeFinalRedirects(unittest.TestCase):
    def test_no_links(self):
        infos = [
            (1, None, None, None, None, 301),
            (2, None, None, None, None, 200),
        ]
        s = [
            (1, 'a', 0, 2, ''),
            (1, 'a', 0, 2, ''),
            (1, 'a', 0, 5, '')
        ]
        r = compute_final_redirects(infos, s)
        # uid_to_dst, uid_to_ext, uid_nb_hops, uid_in_loop = FinalRedirectionStreamDef.compute_final_redirections(s)
        self.assertEqual(len(r.uid_to_dst), 0)
        self.assertEqual(len(r.uid_to_ext), 0)
        self.assertEqual(len(r.uid_nb_hops), 0)
        self.assertEqual(len(r.uid_in_loop), 0)

    def test_links_1(self):
        infos = [
            (1, None, None, None, None, 301),
            (2, None, None, None, None, 200),
        ]
        s = [
            (1, 'r301', 0, 2, ''),
        ]
        r = compute_final_redirects(infos, s)
        self.assertEqual(len(r.uid_to_dst), 1)
        self.assertEqual(len(r.uid_to_ext), 0)
        self.assertEqual(len(r.uid_nb_hops), 1)
        self.assertEqual(len(r.uid_in_loop), 0)

        self.assertEqual(r.uid_to_dst[1], 2)
        self.assertEqual(r.uid_to_http_code.get(1, 200), 200)
        self.assertEqual(r.uid_nb_hops[1], 1)

    def test_links_2(self):
        infos = [
            (1, None, None, None, None, 301),
            (2, None, None, None, None, 301),
            (3, None, None, None, None, 404),
        ]
        s = [
            (1, 'r301', 0, 2, ''),
            (2, 'r301', 0, 3, ''),
        ]
        r = compute_final_redirects(infos, s)
        self.assertEqual(len(r.uid_to_dst), 2)
        self.assertEqual(len(r.uid_to_ext), 0)
        self.assertEqual(len(r.uid_nb_hops), 2)
        self.assertEqual(len(r.uid_in_loop), 0)

        self.assertEqual(r.uid_to_dst[1], 3)
        self.assertEqual(r.uid_nb_hops[1], 2)

        self.assertEqual(r.uid_to_dst[2], 3)
        self.assertEqual(r.uid_nb_hops[2], 1)

    def test_links_3(self):
        infos = [
            (1, None, None, None, None, 301),
            (2, None, None, None, None, 200),
            (3, None, None, None, None, 503),
        ]
        s = [
            (1, 'r301', 0, 3, ''),
            (2, 'r301', 0, 1, ''),
        ]
        r = compute_final_redirects(infos, s)
        self.assertEqual(len(r.uid_to_dst), 2)
        self.assertEqual(len(r.uid_to_ext), 0)
        self.assertEqual(len(r.uid_nb_hops), 2)
        self.assertEqual(len(r.uid_in_loop), 0)

        self.assertEqual(r.uid_to_dst[1], 3)
        self.assertEqual(r.uid_nb_hops[1], 1)

        self.assertEqual(r.uid_to_dst[2], 3)
        self.assertEqual(r.uid_nb_hops[2], 2)

    def test_links_ext_1(self):
        infos = [
            (1, None, None, None, None, 301),
            (2, None, None, None, None, 200),
        ]
        s = [
            (1, 'r301', 0, 2, ''),
            (2, 'r301', 8, -1, 'http://www.example.com/'),
        ]
        r = compute_final_redirects(infos, s)
        self.assertEqual(len(r.uid_to_dst), 2)
        self.assertEqual(len(r.uid_to_ext), 2)
        self.assertEqual(len(r.uid_nb_hops), 2)
        self.assertEqual(len(r.uid_in_loop), 0)

        self.assertEqual(r.uid_to_dst[1], -1)
        self.assertEqual(r.uid_nb_hops[1], 2)

        self.assertEqual(r.uid_to_dst[2], -1)
        self.assertEqual(r.uid_nb_hops[2], 1)

    def test_links_loop_1(self):
        infos = [
            (1, None, None, None, None, 301),
            (2, None, None, None, None, 200),
        ]
        s = [
            (1, 'r301', 0, 2, ''),
            (2, 'r301', 0, 1, ''),
        ]
        r = compute_final_redirects(infos, s)
        self.assertEqual(len(r.uid_to_dst), 2)
        self.assertEqual(len(r.uid_to_ext), 0)
        self.assertEqual(len(r.uid_nb_hops), 2)
        self.assertEqual(len(r.uid_in_loop), 2)

        # self.assertEqual(r.uid_to_dst[1], 2)
        # self.assertEqual(r.uid_nb_hops[1], 2)
        self.assertIn(1, r.uid_in_loop)

        # self.assertEqual(r.uid_to_dst[2], 1)
        # self.assertEqual(r.uid_nb_hops[2], 2)
        self.assertIn(2, r.uid_in_loop)


class TestRedirectFinal(unittest.TestCase):
    def test_no_links(self):
        infos = [
            (1, None, None, None, None, 301),
            (2, None, None, None, None, 200),
        ]
        s = [
            (1, 'a', 0, 2, ''),
            (1, 'a', 0, 2, ''),
            (1, 'a', 0, 5, '')
        ]
        with compute_final_redirects(infos, s) as results:
            self.assertEqual(len(list(results)), 0)

    def test_links_1(self):
        infos = [
            (1, None, None, None, None, 301),
            (2, None, None, None, None, 200),
        ]
        s = [
            (1, 'r301', 0, 2, ''),
        ]
        with compute_final_redirects(infos, s) as results:
            results = iter(results)
            r = results.next()
            self.assertIsInstance(r, RedirectFinal)
            self.assertEqual(r.uid, 1)
            self.assertEqual(r.dst, 2)
            self.assertEqual(r.http_code, 200)
            self.assertEqual(r.nb_hops, 1)
            self.assertFalse(r.in_loop)
            with self.assertRaises(StopIteration):
                results.next()

    def test_links_2(self):
        infos = [
            (1, None, None, None, None, 301),
            (2, None, None, None, None, 301),
            (3, None, None, None, None, 404),
        ]
        s = [
            (1, 'r301', 0, 2, ''),
            (2, 'r301', 0, 3, ''),
        ]
        with compute_final_redirects(infos, s) as results:
            results = iter(results)
            r = results.next()
            self.assertIsInstance(r, RedirectFinal)
            self.assertEqual(r.uid, 1)
            self.assertEqual(r.dst, 3)
            self.assertEqual(r.http_code, 404)
            self.assertEqual(r.nb_hops, 2)
            self.assertFalse(r.in_loop)
            r = results.next()
            self.assertIsInstance(r, RedirectFinal)
            self.assertEqual(r.uid, 2)
            self.assertEqual(r.dst, 3)
            self.assertEqual(r.http_code, 404)
            self.assertEqual(r.nb_hops, 1)
            self.assertFalse(r.in_loop)
            with self.assertRaises(StopIteration):
                results.next()

    def test_links_3(self):
        infos = [
            (1, None, None, None, None, 301),
            (2, None, None, None, None, 200),
            (3, None, None, None, None, 503),
        ]
        s = [
            (1, 'r301', 0, 3, ''),
            (2, 'r301', 0, 1, ''),
        ]
        with compute_final_redirects(infos, s) as results:
            results = iter(results)
            r = results.next()
            self.assertIsInstance(r, RedirectFinal)
            self.assertEqual(r.uid, 1)
            self.assertEqual(r.dst, 3)
            self.assertEqual(r.http_code, 503)
            self.assertEqual(r.nb_hops, 1)
            self.assertFalse(r.in_loop)
            r = results.next()
            self.assertIsInstance(r, RedirectFinal)
            self.assertEqual(r.uid, 2)
            self.assertEqual(r.dst, 3)
            self.assertEqual(r.http_code, 503)
            self.assertEqual(r.nb_hops, 2)
            self.assertFalse(r.in_loop)
            with self.assertRaises(StopIteration):
                results.next()

    def test_links_ext_1(self):
        infos = [
            (1, None, None, None, None, 301),
            (2, None, None, None, None, 200),
        ]
        s = [
            (1, 'r301', 0, 2, ''),
            (2, 'r301', 8, -1, 'http://www.example.com/'),
        ]
        with compute_final_redirects(infos, s) as results:
            results = iter(results)
            r = results.next()
            self.assertIsInstance(r, RedirectFinal)
            self.assertEqual(r.uid, 1)
            self.assertEqual(r.dst, -1)
            self.assertEqual(r.ext, 'http://www.example.com/')
            self.assertFalse(r.http_code)
            self.assertEqual(r.nb_hops, 2)
            r = results.next()
            self.assertIsInstance(r, RedirectFinal)
            self.assertEqual(r.uid, 2)
            self.assertEqual(r.dst, -1)
            self.assertEqual(r.ext, 'http://www.example.com/')
            self.assertFalse(r.http_code)
            self.assertEqual(r.nb_hops, 1)
            with self.assertRaises(StopIteration):
                results.next()

    def test_links_loop_1(self):
        infos = [
            (1, None, None, None, None, 301),
            (2, None, None, None, None, 200),
        ]
        s = [
            (1, 'r301', 0, 2, ''),
            (2, 'r301', 0, 1, ''),
        ]
        with compute_final_redirects(infos, s) as results:
            results = iter(results)
            r = results.next()
            self.assertIsInstance(r, RedirectFinal)
            self.assertEqual(r.uid, 1)
            self.assertTrue(r.in_loop)
            r = results.next()
            self.assertIsInstance(r, RedirectFinal)
            self.assertEqual(r.uid, 2)
            self.assertTrue(r.in_loop)
            with self.assertRaises(StopIteration):
                results.next()
