__author__ = 'zeb'

import unittest

from cdf.features.links.redirect_final import (
    FinalRedirectionStreamDef,
    RedirectFinal,
)

# Start of InfoStreamDef:
# ('id', int),
# ('infos_mask', int),
# ('content_type', str),
# ('depth', int),
# ('date_crawled', int),
# ('http_code', int),

class TestFinalRedirectionStreamDef(unittest.TestCase):
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
        r = FinalRedirectionStreamDef.compute_final_redirections(infos, s)
        #uid_to_dst, uid_to_ext, uid_nb_hops, uid_in_loop = FinalRedirectionStreamDef.compute_final_redirections(s)
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
        r = FinalRedirectionStreamDef.compute_final_redirections(infos, s)
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
        r = FinalRedirectionStreamDef.compute_final_redirections(infos, s)
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
        r = FinalRedirectionStreamDef.compute_final_redirections(infos, s)
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
        r = FinalRedirectionStreamDef.compute_final_redirections(infos, s)
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
        r = FinalRedirectionStreamDef.compute_final_redirections(infos, s)
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
        with FinalRedirectionStreamDef.compute_final_redirections(infos, s) as results:
            self.assertEqual(len(list(results)), 0)

    def test_links_1(self):
        infos = [
            (1, None, None, None, None, 301),
            (2, None, None, None, None, 200),
        ]
        s = [
            (1, 'r301', 0, 2, ''),
        ]
        with FinalRedirectionStreamDef.compute_final_redirections(infos, s) as results:
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
        with FinalRedirectionStreamDef.compute_final_redirections(infos, s) as results:
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
        with FinalRedirectionStreamDef.compute_final_redirections(infos, s) as results:
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
        with FinalRedirectionStreamDef.compute_final_redirections(infos, s) as results:
            results = iter(results)
            r = results.next()
            self.assertIsInstance(r, RedirectFinal)
            self.assertEqual(r.uid, 1)
            self.assertIsNone(r.dst)
            self.assertEqual(r.ext, 'http://www.example.com/')
            self.assertIsNone(r.http_code)
            self.assertEqual(r.nb_hops, 2)
            r = results.next()
            self.assertIsInstance(r, RedirectFinal)
            self.assertEqual(r.uid, 2)
            self.assertIsNone(r.dst)
            self.assertEqual(r.ext, 'http://www.example.com/')
            self.assertIsNone(r.http_code)
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
        with FinalRedirectionStreamDef.compute_final_redirections(infos, s) as results:
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
