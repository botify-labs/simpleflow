import unittest
from cdf.features.duplicate_query_kvs.get_urls import get_urls_with_same_kv
from cdf.features.duplicate_query_kvs.streams import DuplicateQueryKVsStreamDef
from cdf.features.main.streams import IdStreamDef
from cdf.tasks.documents import UrlDocumentGenerator

urlids = [
    (0, 'http', 'www.example.com', '/page.html', ''),
    (1, 'http', 'www.example.com', '/page.html', '?x=a&y=b&z=3'),
    (2, 'http', 'www.example.com', '/page.html', '?y=b&z=3&x=a'),
    (3, 'http', 'www.example.com', '/page.html', '?z=3&y=b&x=a'),
]

duplicate_query_kvs = [(1, '2 3'), (2, '1 3'), (3, '1 2')]


class TestTask(unittest.TestCase):
    def test_get_urls_with_same_kv(self):
        max_crawled_urlid = urlids[-1][0]
        prob_uids = iter(get_urls_with_same_kv(urlids, max_crawled_urlid))
        pu = next(prob_uids)
        self.assertEqual((1, '2 3'), pu)
        pu = next(prob_uids)
        self.assertEqual((2, '1 3'), pu)
        pu = next(prob_uids)
        self.assertEqual((3, '1 2'), pu)
        with self.assertRaises(StopIteration):
            next(prob_uids)
        p = list(get_urls_with_same_kv(urlids, max_crawled_urlid))
        self.assertEqual(duplicate_query_kvs, p)


class TestDocument(unittest.TestCase):
    def test_doc(self):
        gen = UrlDocumentGenerator([
            IdStreamDef.load_iterator(iter(urlids)),
            DuplicateQueryKVsStreamDef.load_iterator(iter(duplicate_query_kvs))
        ])
        documents = list(gen)
        self.assertEqual(4, len(documents))
        d = documents[0][1]
        self.assertIn('duplicate_query_kvs', d)
        d = documents[1][1]
        self.assertIn('duplicate_query_kvs', d)
        self.assertIn('nb', d['duplicate_query_kvs'])
        self.assertEqual(3, d['duplicate_query_kvs']['nb'])
        self.assertEqual([2, 3], d['duplicate_query_kvs']['urls'])


if __name__ == '__main__':
    unittest.main()
