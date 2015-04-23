__author__ = 'zeb'

import unittest

from cdf.features.links.redirect_final import (
    FinalRedirectionStreamDef,
)

class TestFinalRedirectionStreamDef(unittest.TestCase):
    def test_no_links(self):
        s = iter([
            (1, 'a', 0, 2, ''),
            (1, 'a', 0, 2, ''),
            (1, 'a', 0, 5, '')
        ])
        uid_to_dst, uid_to_ext, uid_nb_hops, uid_in_loop = FinalRedirectionStreamDef.compute_final_redirections(s)
        self.assertEqual(len(uid_to_dst), 0)
        self.assertEqual(len(uid_to_ext), 0)
        self.assertEqual(len(uid_nb_hops), 0)
        self.assertEqual(len(uid_in_loop), 0)

    def test_links_1(self):
        s = iter([
            (1, 'r301', 0, 2, ''),
        ])
        uid_to_dst, uid_to_ext, uid_nb_hops, uid_in_loop = FinalRedirectionStreamDef.compute_final_redirections(s)
        self.assertEqual(len(uid_to_dst), 1)
        self.assertEqual(len(uid_to_ext), 0)
        self.assertEqual(len(uid_nb_hops), 1)
        self.assertEqual(len(uid_in_loop), 0)

        self.assertEqual(uid_to_dst[1], 2)
        self.assertEqual(uid_nb_hops[1], 2)

    def test_links_2(self):
        s = iter([
            (1, 'r301', 0, 2, ''),
            (2, 'r301', 0, 3, ''),
        ])
        uid_to_dst, uid_to_ext, uid_nb_hops, uid_in_loop = FinalRedirectionStreamDef.compute_final_redirections(s)
        self.assertEqual(len(uid_to_dst), 2)
        self.assertEqual(len(uid_to_ext), 0)
        self.assertEqual(len(uid_nb_hops), 2)
        self.assertEqual(len(uid_in_loop), 0)

        self.assertEqual(uid_to_dst[1], 3)
        self.assertEqual(uid_nb_hops[1], 3)

        self.assertEqual(uid_to_dst[2], 3)
        self.assertEqual(uid_nb_hops[2], 2)

    def test_links_3(self):
        s = iter([
            (1, 'r301', 0, 3, ''),
            (2, 'r301', 0, 1, ''),
        ])
        uid_to_dst, uid_to_ext, uid_nb_hops, uid_in_loop = FinalRedirectionStreamDef.compute_final_redirections(s)
        self.assertEqual(len(uid_to_dst), 2)
        self.assertEqual(len(uid_to_ext), 0)
        self.assertEqual(len(uid_nb_hops), 2)
        self.assertEqual(len(uid_in_loop), 0)

        self.assertEqual(uid_to_dst[1], 3)
        self.assertEqual(uid_nb_hops[1], 2)

        self.assertEqual(uid_to_dst[2], 3)
        self.assertEqual(uid_nb_hops[2], 3)

    def test_links_ext_1(self):
        s = iter([
            (1, 'r301', 0, 2, ''),
            (2, 'r301', 8, -1, 'http://www.example.com/'),
        ])
        uid_to_dst, uid_to_ext, uid_nb_hops, uid_in_loop = FinalRedirectionStreamDef.compute_final_redirections(s)
        self.assertEqual(len(uid_to_dst), 2)
        self.assertEqual(len(uid_to_ext), 2)
        self.assertEqual(len(uid_nb_hops), 2)
        self.assertEqual(len(uid_in_loop), 0)

        self.assertEqual(uid_to_dst[1], -1)
        self.assertEqual(uid_nb_hops[1], 3)

        self.assertEqual(uid_to_dst[2], -1)
        self.assertEqual(uid_nb_hops[2], 2)

    def test_links_loop_1(self):
        s = iter([
            (1, 'r301', 0, 2, ''),
            (2, 'r301', 0, 1, ''),
        ])
        uid_to_dst, uid_to_ext, uid_nb_hops, uid_in_loop = FinalRedirectionStreamDef.compute_final_redirections(s)
        self.assertEqual(len(uid_to_dst), 2)
        self.assertEqual(len(uid_to_ext), 0)
        self.assertEqual(len(uid_nb_hops), 2)
        self.assertEqual(len(uid_in_loop), 2)

        # self.assertEqual(uid_to_dst[1], 2)
        # self.assertEqual(uid_nb_hops[1], 2)
        self.assertIn(1, uid_in_loop)

        # self.assertEqual(uid_to_dst[2], 1)
        # self.assertEqual(uid_nb_hops[2], 2)
        self.assertIn(2, uid_in_loop)
