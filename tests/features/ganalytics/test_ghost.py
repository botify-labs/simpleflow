import unittest
import mock

import heapq
from collections import Counter

from cdf.features.ganalytics.ghost import (get_medium_sources,
                                           update_session_count,
                                           update_top_ghost_pages,
                                           update_urls_count,
                                           build_ghost_counts_dict,
                                           save_ghost_pages,
                                           save_ghost_pages_count)


class TestGetMediumSources(unittest.TestCase):
    def test_nominal_case(self):
        self.assertEqual(["organic.all", "organic.google"],
                         get_medium_sources("organic", "google", None))
        self.assertEqual(["social.all", "social.facebook"],
                         get_medium_sources("social", "facebook.com", "facebook"))
        self.assertEqual(["social.all"],
                         get_medium_sources("social", "facebook.com", "foo"))


class TestUpdateSessionCount(unittest.TestCase):
    def setUp(self):
        #some RawVisitsStreamDef fields are missing,
        #but it should be ok for the function
        self.entry = ["foo", "organic", "bing", None, 5]
        self.url = "foo"
        self.medium = "organic"
        self.source = "bing"
        self.social_network = None
        self.nb_sessions = 5

    def test_nominal_case(self):
        ghost_pages = {
            "organic.all": 9,
            "organic.google": 5,
            "organic.bing": 4
        }
        ghost_pages = Counter(ghost_pages)
        update_session_count(ghost_pages, self.medium, self.source,
                             self.social_network, self.nb_sessions)

        expected_ghost_pages = {
            "organic.all": 14,
            "organic.google": 5,
            "organic.bing": 9
        }
        self.assertEqual(expected_ghost_pages, ghost_pages)

    def test_missing_medium(self):
        #organic key is missing. This should never happen at runtime
        #since bing is an organic source
        ghost_pages = {
            "organic.bing": 4
        }
        ghost_pages = Counter(ghost_pages)
        update_session_count(ghost_pages, self.medium, self.source,
                             self.social_network, self.nb_sessions)

        expected_ghost_pages = {
            "organic.all": 5,
            "organic.bing": 9
        }
        self.assertEqual(expected_ghost_pages, ghost_pages)

    def test_missing_source(self):
        #there is currently no key for bing
        ghost_pages = {
            "organic.all": 9
        }
        ghost_pages = Counter(ghost_pages)
        update_session_count(ghost_pages, self.medium, self.source,
                             self.social_network, self.nb_sessions)

        expected_ghost_pages = {
            "organic.all": 14,
            "organic.bing": 5
        }
        self.assertEqual(expected_ghost_pages, ghost_pages)


class TestUpdateUrlCount(unittest.TestCase):
    def setUp(self):
        #some RawVisitsStreamDef fields are missing,
        #but it should be ok for the function
        self.entry = ["foo", "organic", "bing", None, 5]
        self.url = "foo"
        self.medium = "organic"
        self.source = "bing"
        self.social_network = None
        self.nb_sessions = 5

    def test_nominal_case(self):
        ghost_pages = {
            "organic.all": 0,
            "organic.google": 0,
            "organic.bing": 0
        }
        update_urls_count(ghost_pages, self.medium, self.source,
                          self.social_network)

        expected_ghost_pages = {
            "organic.all": 1,
            "organic.google": 0,
            "organic.bing": 1
        }
        self.assertEqual(expected_ghost_pages, ghost_pages)

    def test_counter_already_set(self):
        ghost_pages = {
            "organic.all": 1,
            "organic.google": 0,
            "organic.bing": 0
        }
        update_urls_count(ghost_pages, self.medium, self.source,
                          self.social_network)

        expected_ghost_pages = {
            "organic.all": 1,
            "organic.google": 0,
            "organic.bing": 1
        }
        self.assertEqual(expected_ghost_pages, ghost_pages)


class TestUpdateTopGhostPages(unittest.TestCase):
    def test_nominal_case(self):
        top_ghost_pages = {
            "organic": [(8, "foo"), (4, "bar")],
            "google": [(5, "foo")],
            "social": [(10, "foo"), (5, "bar")],
        }
        for value in top_ghost_pages.itervalues():
            heapq.heapify(value)

        nb_top_ghost_pages = 2
        url = "baz"
        session_count = {
            "organic": 6,
            "google": 4,
            "social": 2
        }

        update_top_ghost_pages(top_ghost_pages, nb_top_ghost_pages,
                               url, session_count)

        expected_result = {
            #baz is now in the top 2 organic ghost pages
            "organic": [(8, "foo"), (6, "baz")],
            #baz is now in the top 2 google ghost pages
            "google": [(5, "foo"), (4, "baz")],
            #baz is NOT in the top 2  ghost pages
            "social": [(10, "foo"), (5, "bar")],
        }
        for value in expected_result.itervalues():
            heapq.heapify(value)

        self.assertEqual(expected_result, top_ghost_pages)

    def test_missing_source(self):
        top_ghost_pages = {}

        nb_top_ghost_pages = 2
        url = "foo"
        session_count = {
            "organic": 6,
        }

        update_top_ghost_pages(top_ghost_pages, nb_top_ghost_pages,
                               url, session_count)

        expected_result = {
            "organic": [(6, "foo")],
        }

        self.assertEqual(expected_result, top_ghost_pages)


class TestBuildGhostCountsDict(unittest.TestCase):
    def test_nominal_case(self):
        session_count = {
            "organic.all": 100,
            "organic.google": 70,
        }

        url_count = {
            "organic.all": 80,
            "organic.google": 10,
            "organic.bing": 5
        }
        actual_result = build_ghost_counts_dict(session_count, url_count)
        expected_result = {
            "organic.all.nb_visits": 100,
            "organic.all.nb_urls": 80,
            "organic.google.nb_visits": 70,
            "organic.google.nb_urls": 10,
            "organic.bing.nb_urls": 5
        }
        self.assertEqual(expected_result, actual_result)


class TestSaveGhostPages(unittest.TestCase):
    @mock.patch('__builtin__.open')
    def test_nominal_case(self, mock_open):
        mock_open.return_value = mock.MagicMock(spec=file)

        source = "organic"
        ghost_pages = [(9, "foo"), (2, "bar")]
        output_dir = "/tmp/tests"
        save_ghost_pages(source, ghost_pages, output_dir)

        #test that the correct file was open
        mock_open.assert_call_with("/tmp/tests/top_ghost_pages_organic.tsv")

        #test what is written in the file
        file_handle = mock_open.return_value.__enter__.return_value
        self.assertEqual([mock.call('foo\t9\n'),
                          mock.call('bar\t2\n')],
                         file_handle.write.call_args_list)


class TestSaveGhostPagesSessionCount(unittest.TestCase):
    @mock.patch('__builtin__.open')
    def test_nominal_case(self, mock_open):
        mock_open.return_value = mock.MagicMock(spec=file)

        session_count = {
            "organic.all": 10,
            "social.all": 5
        }
        output_dir = "/tmp/tests"
        save_ghost_pages_count(session_count, output_dir)

        #test that the correct file was open
        mock_open.assert_call_with("/tmp/tests/ghost_pages_count.json")

        #test what is written in the file
        file_handle = mock_open.return_value.__enter__.return_value
        self.assertEqual([mock.call('{"organic": {"all": 10}, "social": {"all": 5}}')],
                         file_handle.write.call_args_list)
