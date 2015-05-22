import tempfile
import unittest
import shutil
from cdf.features.links.bad_links import get_links_to_non_canonical_counters, \
    gen_links_to_non_canonical, get_bad_canonicals
from cdf.features.links.streams import OutlinksRawStreamDef, LinksToNonCanonicalStreamDef
from cdf.features.links.tasks import make_links_to_non_canonical_file
from cdf.query.datamodel import get_fields

def get_links_to_non_canonical(g):
    bad_canonicals = get_bad_canonicals(
        g())
    generator = gen_links_to_non_canonical(
        g(False),
        bad_canonicals
    )
    return list(generator)


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
        # Data copied from another test
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
        links = get_links_to_non_canonical(lambda x=False: iter(stream_outlinks))
        self.assertEqual(0, len(links))

    def test_bad(self):
        stream_outlinks = [
            (1, 'a', 0, 2, ''),
            (2, 'canonical', 0, 3, ''),
            (3, 'canonical', 0, 3, ''),
        ]
        links = get_links_to_non_canonical(lambda x=False: iter(stream_outlinks))
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
        links = get_links_to_non_canonical(lambda x=False: iter(stream_outlinks))
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
        links = get_links_to_non_canonical(lambda x=False: iter(stream_outlinks))
        counters = get_links_to_non_canonical_counters(links)
        c = counters.next()
        self.assertEqual((1, 1), c)
        c = counters.next()
        self.assertEqual((4, 2), c)
        with self.assertRaises(StopIteration):
            counters.next()

class TestWithFiles(unittest.TestCase):
    tmp_dir = tempfile.mkdtemp()
    @classmethod
    def setUpClass(cls):
        # tests use `tmp_dir`
        stream_outlinks = [
            (1, 'a', 0, 2, ''),
            (2, 'canonical', 0, 3, ''),
            (3, 'canonical', 0, 3, ''),
            (4, 'a', 0, 2, ''),
            (4, 'a', 0, 5, ''),
            (5, 'canonical', 0, 3, ''),
        ]
        OutlinksRawStreamDef.persist(stream_outlinks, cls.tmp_dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp_dir)

    def test_bad_with_stream(self):
        make_links_to_non_canonical_file(self.tmp_dir)
        links = LinksToNonCanonicalStreamDef.load(self.tmp_dir)
        it = links.next()
        self.assertEqual([1, True, 2], it)
        it = links.next()
        self.assertEqual([4, True, 2], it)
        it = links.next()
        self.assertEqual([4, True, 5], it)
        with self.assertRaises(StopIteration):
            links.next()
