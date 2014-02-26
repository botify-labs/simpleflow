import unittest
from cdf.analysis.urls.transducers.links import OutlinksTransducer, InlinksTransducer


class TestLinkCounters(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def assertNonOrderedList(self, expected, result):
        self.assertEqual(len(expected), len(result))
        for entry in expected:
            self.assertTrue(entry in result)


    def test_out_links(self):
        """Test for out links aggregation

        Output format:
            (url_id, 'links', bitmask, is_internal, nb_links, nb_links_unique)
        """
        stream_outlinks = [
            [1, 'a', 0, 2, ''],
            [1, 'a', 1, 3, ''],
            [1, 'a', 0, 4, ''],
            [1, 'a', 0, 4, ''],
            [1, 'a', 1, 4, ''],
            [1, 'a', 0, -1, 'http://www.youtube.com'],
            [1, 'a', 0, -1, 'http://www.youtube.com'],
            [1, 'a', 4, -1, 'http://www.lemonde.com'], # internal
            [3, 'a', 0, -1, 'http://www.youtube.com'],
            [3, 'a', 5, 5, ''],
            [3, 'a', 5, 5, ''],
        ]

        result = list(OutlinksTransducer(stream_outlinks).get())

        expected = [
            (1, 'links', 0, 1, 3, 2),
            (1, 'links', 1, 1, 2, 2),
            (1, 'links', 0, 0, 2, 1),
            (1, 'links', 4, 1, 1, 1),
            (3, 'links', 0, 0, 1, 1),
            (3, 'links', 5, 1, 2, 1)
        ]

        self.assertNonOrderedList(expected, result)

    def test_out_canonicals(self):
        """Test for out canonical link aggregation

        Output format:
            (url_id, 'canonical', is_equal)
            is_equal is 1 if the canonical url is the url itself
        """
        stream_outlinks = [
            [1, 'r301', 0, 5, ''],
            [2, 'canonical', 0, 4, ''],
            [2, 'canonical', 0, 2, ''], # should be ignored
            [2, 'canonical', 0, 11, ''], # should be ignored
            [3, 'canonical', 0, 4, ''],
            [4, 'a', 0, 5, ''],
            [4, 'a', 0, 5, ''],
            [4, 'a', 0, 5, ''],
            [4, 'canonical', 0, 4, ''],
            [6, 'r301', 4, -1, 'http://www.lemonde.com']  # internal
        ]

        result = list(OutlinksTransducer(stream_outlinks).get())

        expected = [
            (1, 'redirect', 1),
            (2, 'canonical', 0),
            (3, 'canonical', 0),
            (4, 'links', 0, 1, 3, 1),
            (4, 'canonical', 1),
            (6, 'redirect', 1)
        ]

        self.assertNonOrderedList(expected, result)

    def test_out_redirects(self):
        """Test for out redirecgtion aggregation

        Output format:
            (url_id, 'redirect', is_internal)
        """
        stream_outlinks = [
            [1, 'r301', 0, 5, ''],
            [2, 'r302', 1, -1, 'http://www.youtube.com'],
            [4, 'a', 0, 5, ''],
            [4, 'a', 0, 5, ''],
            [4, 'a', 0, 5, ''],
            [6, 'r301', 4, -1, 'http://www.lemonde.com']  # internal
        ]

        result = list(OutlinksTransducer(stream_outlinks).get())

        expected = [
            (1, 'redirect', 1),
            (2, 'redirect', 0),
            (4, 'links', 0, 1, 3, 1),
            (6, 'redirect', 1)
        ]

        self.assertNonOrderedList(expected, result)

    def test_in_links(self):
        stream_inlinks = [
            [1, 'a', 0, 2],
            [1, 'a', 1, 3],
            [1, 'a', 0, 4],
            [1, 'a', 1, 4],
            [1, 'a', 5, 4],
            [1, 'a', 5, 4],
            [3, 'a', 5, 5],
            [3, 'a', 5, 5],
        ]

        result = list(InlinksTransducer(stream_inlinks).get())

        expected = [
            (1, 'links', 0, 2, 2),
            (1, 'links', 1, 2, 2),
            (1, 'links', 5, 2, 1),
            (3, 'links', 5, 2, 1)
        ]

        self.assertNonOrderedList(expected, result)

    def test_in_redirects(self):
        stream_inlinks = [
            [1, 'r301', 0, 2],
            [1, 'r302', 0, 3],
            [1, 'r303', 5, 4],
            [2, 'a', 0, 1],
            [3, 'r302', 5, 5],
        ]

        result = list(InlinksTransducer(stream_inlinks).get())

        expected = [
            (1, 'redirect', 3),
            (2, 'links', 0, 1, 1),
            (3, 'redirect', 1)
        ]

        self.assertNonOrderedList(expected, result)

    def test_in_canonicals(self):
        stream_inlinks = [
            [1, 'canonical', 24, 1],  # self canonical
            [1, 'canonical', 0, 2],
            [1, 'canonical', 0, 2],
            [1, 'canonical', 0, 2],
            [1, 'canonical', 0, 3],
            [1, 'canonical', 5, 4],
            [2, 'canonical', 0, 2],  # self canonical
            [2, 'canonical', 17, 1],
            [3, 'canonical', 5, 5],
            [3, 'canonical', 8, 1],  # first canonical of url 1
            [4, 'canonical', 16, 1]
        ]

        result = list(InlinksTransducer(stream_inlinks).get())

        # the first canonical of url 1 is set to url 3
        expected = [
            (1, 'canonical', 3),
            (3, 'canonical', 2)
        ]

        self.assertNonOrderedList(expected, result)
