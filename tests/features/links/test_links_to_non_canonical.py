import unittest
from cdf.features.links.bad_links import get_links_to_non_canonical, get_links_to_non_canonical_counters
from cdf.query.datamodel import get_fields


class TestFeature(unittest.TestCase):
    def test_features_options_1(self):
        features_options = {
            'links': {
            },
        }
        fields = get_fields(features_options, remove_admin=False)
        bc = filter(lambda f: f['value'].startswith('outlinks_errors.bad_canonical.'), fields)
        self.assertEqual(0, len(bc))

    def test_features_options_2(self):
        features_options = {
            'links': {
                'links_to_non_canonical': True,
            },
        }
        fields = get_fields(features_options, remove_admin=False)
        bc = filter(lambda f: f['value'].startswith('outlinks_errors.bad_canonical.'), fields)
        self.assertEqual(2, len(bc))


class TestLinkToNonCanonical(unittest.TestCase):
    def test_ok(self):
        # Copied from another test
        stream_outlinks = [
            [1, 'r301', 0, 5, ''],
            [2, 'canonical', 0, 4, ''],
            [2, 'canonical', 0, 2, ''],  # should be ignored
            [2, 'canonical', 0, 11, ''],  # should be ignored
            [3, 'canonical', 0, 4, ''],
            [4, 'a', 0, 5, ''],
            [4, 'a', 0, 5, ''],
            [4, 'a', 0, 5, ''],
            [4, 'canonical', 0, 4, ''],
            [6, 'r301', 4, -1, 'http://www.lemonde.com']  # internal
        ]
        links = get_links_to_non_canonical(stream_outlinks)
        self.assertEqual(0, len(links))

    def test_bad(self):
        stream_outlinks = [
            (1, 'a', 0, 2, ''),
            (2, 'canonical', 0, 3, ''),
            (3, 'canonical', 0, 3, ''),
        ]
        links = get_links_to_non_canonical(stream_outlinks)
        self.assertEqual(1, len(links))

        self.assertEqual(1, links[0][0])
        self.assertTrue(links[0][1])
        self.assertEqual(2, links[0][2])


class TestLinkToNonCanonicalCounters(unittest.TestCase):
    def test_ok(self):
        # Copied from another test
        stream_outlinks = [
            [1, 'r301', 0, 5, ''],
            [2, 'canonical', 0, 4, ''],
            [2, 'canonical', 0, 2, ''],  # should be ignored
            [2, 'canonical', 0, 11, ''],  # should be ignored
            [3, 'canonical', 0, 4, ''],
            [4, 'a', 0, 5, ''],
            [4, 'a', 0, 5, ''],
            [4, 'a', 0, 5, ''],
            [4, 'canonical', 0, 4, ''],
            [6, 'r301', 4, -1, 'http://www.lemonde.com']  # internal
        ]
        links = get_links_to_non_canonical(stream_outlinks)
        counters = list(get_links_to_non_canonical_counters(links))
        self.assertEqual(0, len(counters))

    def test_bad(self):
        stream_outlinks = [
            (1, 'a', 0, 2, ''),
            (2, 'canonical', 0, 3, ''),
            (3, 'canonical', 0, 3, ''),
            (4, 'a', 0, 2, ''),
            (4, 'a', 0, 5, ''),
            (5, 'canonical', 0, 3, ''),
        ]
        links = get_links_to_non_canonical(stream_outlinks)
        counters = get_links_to_non_canonical_counters(links)
        c = counters.next()
        self.assertEqual((1, 1), c)
        c = counters.next()
        self.assertEqual((4, 2), c)
        with self.assertRaises(StopIteration):
            counters.next()
