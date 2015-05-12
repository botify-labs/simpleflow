import unittest
from cdf.features.main.streams import IdStreamDef
from cdf.features.semantic_metadata.streams import ContentsStreamDef
from cdf.query.datamodel import get_fields
from cdf.tasks.documents import UrlDocumentGenerator


class TestLen(unittest.TestCase):
    def test_features_options_1(self):
        features_options = {
        }
        fields = get_fields(features_options, remove_admin=False)
        lens = filter(lambda f:f['value'].endswith('.len'), fields)
        self.assertEqual(0, len(lens))

    def test_features_options_2(self):
        features_options = {
            'semantic_metadata': {
            },
        }
        fields = get_fields(features_options, remove_admin=False)
        lens = filter(lambda f:f['value'].endswith('.len'), fields)
        self.assertEqual(0, len(lens))

    def test_features_options_3(self):
        features_options = {
            'semantic_metadata': {
                'length': False,
            },
        }
        fields = get_fields(features_options, remove_admin=False)
        lens = filter(lambda f:f['value'].endswith('.len'), fields)
        self.assertEqual(0, len(lens))

    def test_features_options_4(self):
        features_options = {
            'semantic_metadata': {
                'length': True,
            },
        }
        fields = get_fields(features_options, remove_admin=False)
        lens = filter(lambda f:f['value'].endswith('.len'), fields)
        self.assertEqual(3, len(lens))

    def test_1(self):
        ids = [
            [1, 'http', 'www.site.com', '/path/name.html', ''],
            [2, 'http', 'www.site.com', '/path/other_name.html', ''],
        ]
        urlcontents = [
            [1, 1, 0, 'Title'],
            [1, 2, 0, 'First H1'],
            [1, 2, 0, 'Next H1'],
        ]
        features_options = {
            'semantic_metadata': {
                'length': True,
            },
        }
        gen = UrlDocumentGenerator(
            [IdStreamDef.load_iterator(iter(ids)),
             ContentsStreamDef.load_iterator(iter(urlcontents))])
        documents = list(gen)
        meta = documents[0][1]['metadata']
        self.assertIn('len', meta['title'])
        self.assertIn('len', meta['h1'])
        self.assertNotIn('len', meta['description'])
        self.assertEqual(meta['title']['len'], 5)
        self.assertEqual(meta['h1']['len'], len('First H1'))