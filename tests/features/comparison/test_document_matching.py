import gzip
import os
import shutil
import tempfile
import unittest
import mock
import ujson as json
from moto import mock_s3
import boto
from boto.s3.key import Key

from cdf.features.comparison import matching
from cdf.features.comparison.constants import MatchingState
from cdf.features.comparison.exceptions import UrlKeyDecodingError
from cdf.features.comparison.tasks import match_documents
from cdf.utils.s3 import list_files


class TestUrlKeyCoding(unittest.TestCase):
    @classmethod
    @mock.patch('cdf.features.comparison.constants.SEPARATOR', '|')
    def setUpClass(cls):
        reload(matching)

    def test_url_key_encoding(self):
        url = 'http://www.abc.com/'
        url_id = '15'
        result = matching.encode_url_key(url, url_id)
        expected = url + '|' + url_id
        self.assertEqual(result, expected)

    def test_url_key_decoding_normal(self):
        url_key = 'http://www.abc.com/|1234'
        result = matching.decode_url_key(url_key)
        expected = 'http://www.abc.com/', 1234
        self.assertEqual(result, expected)

    def test_url_key_decoding_separator_in_url(self):
        url_key = 'http://www.ab|c.com/|1234'
        result = matching.decode_url_key(url_key)
        expected = 'http://www.ab|c.com/', 1234
        self.assertEqual(result, expected)

    def test_url_key_decoding_error(self):
        url_key = 'http://www.ab|c.com/'
        self.assertRaises(UrlKeyDecodingError,
                          matching.decode_url_key, url_key)

        url_key = 'http://www.abc.com/|'
        self.assertRaises(UrlKeyDecodingError,
                          matching.decode_url_key, url_key)

        url_key = 'http://www.abc.com/|abc'
        self.assertRaises(UrlKeyDecodingError,
                          matching.decode_url_key, url_key)


class TestConversionTable(unittest.TestCase):
    @classmethod
    @mock.patch('cdf.features.comparison.constants.SEPARATOR', '|')
    def setUpClass(cls):
        reload(matching)

    def test_generate_conversion_table(self):
        ref_stream = iter([
            'a|11',
            'c|33',
            'd|44',
        ])

        new_stream = iter([
            'b|2',
            'c|3',
            'd|4',
            'e|5',
        ])

        result = matching.generate_conversion_table(ref_stream, new_stream)
        expected = {33: 3, 44: 4}
        self.assertEqual(result, expected)


class TestUrlIdCorrection(unittest.TestCase):
    def test_correction(self):
        conversion = {1: 11, 2: 22, 3: 33, 4: 44}
        fields = ['a.b', 'c', 'd', 'e', 'f']
        documents = [
            {
                'c': [],
                'd': [[1, True], [2, True]],
                'a': {},
                'f': {'url_str': 'no_correction_needed'}
            },
            {
                'c': [1, 2, 3],
                'd': [[1, True, 0], [9, True, 1]],
                'a': {
                    'b': 5
                },
                'e': {'url_id': 4}
            }
        ]

        expected = [
            {
                'c': [],
                'd': [[11, True], [22, True]],
                'a': {},
                'f': {'url_str': 'no_correction_needed'}
            },
            {
                'c': [11, 22, 33],
                'd': [[11, True, 0], [-9, True, 1]],
                'a': {
                    'b': -5  # no-match url
                },
                'e': {'url_id': 44}
            }
        ]

        result = list(matching.document_url_id_correction(
            iter(documents), conversion_table=conversion,
            correction_fields=fields))

        self.assertEqual(result, expected)


class TestDocumentMatching(unittest.TestCase):
    def setUp(self):
        self.document1 = {'id': 1, 'url': 'a', 'url_hash': 'a'}
        self.document2 = {'id': 2, 'url': 'b', 'url_hash': 'b'}
        self.document3 = {'id': 3, 'url': 'c'}  # not crawled
        self.document4 = {'id': 4, 'url': 'd', 'url_hash': 'd'}
        self.document5 = {'id': 5, 'url': 'e', 'url_hash': 'e'}
        self.document6 = {'id': 6, 'url': 'f'}  # not crawled
        self.document7 = {'id': 7, 'url': 'g'}  # not crawled

    def test_matching_state_ref_longer(self):
        ref_stream = iter([
            self.document1,
            self.document3,
            self.document4,
            self.document6,
            self.document7
        ])

        new_stream = iter([
            self.document2,
            self.document3,
            self.document4,
            self.document5,
        ])

        # inspect only the matching state
        result = [state for state, _ in
                  matching.document_match(ref_stream, new_stream)]

        expected = [
            MatchingState.DISAPPEAR,
            MatchingState.DISCOVER,
            MatchingState.MATCH,
            MatchingState.MATCH,
            MatchingState.DISCOVER,
            MatchingState.DISAPPEAR,
            MatchingState.DISAPPEAR
        ]

        self.assertEqual(expected, result)

    def test_matching_state_new_longer(self):
        ref_stream = iter([
            self.document1,
            self.document3,
            self.document4,
        ])

        new_stream = iter([
            self.document2,
            self.document3,
            self.document4,
            self.document5,
        ])

        # inspect only the matching state
        result = [state for state, _ in
                  matching.document_match(ref_stream, new_stream)]

        expected = [
            MatchingState.DISAPPEAR,
            MatchingState.DISCOVER,
            MatchingState.MATCH,
            MatchingState.MATCH,
            MatchingState.DISCOVER,
        ]

        self.assertEqual(expected, result)

    def test_matching_state_equal_length(self):
        ref_stream = iter([
            self.document1,
            self.document3,
            self.document4,
        ])

        new_stream = iter([
            self.document2,
            self.document3,
            self.document4,
        ])

        # inspect only the matching state
        result = [state for state, _ in
                  matching.document_match(ref_stream, new_stream)]

        expected = [
            MatchingState.DISAPPEAR,
            MatchingState.DISCOVER,
            MatchingState.MATCH,
            MatchingState.MATCH,
        ]

        self.assertEqual(expected, result)

    def test_matching_output(self):
        ref_stream = iter([
            self.document1,
            self.document3,
        ])

        new_stream = iter([
            self.document2,
            self.document3,
        ])

        # inspect only the matching state
        result = [(state, docs) for state, docs in
                  matching.document_match(ref_stream, new_stream)]
        expected = [
            (MatchingState.DISAPPEAR, (self.document1, None)),
            (MatchingState.DISCOVER, (None, self.document2)),
            (MatchingState.MATCH, (self.document3, self.document3)),
        ]

        self.assertEqual(expected, result)

    def test_document_merge(self):
        match_stream = iter([
            (MatchingState.MATCH, (self.document1, self.document2, None)),
            (MatchingState.DISAPPEAR, (self.document3, None, None)),
            (MatchingState.DISCOVER, (None, self.document4, None)),
        ])
        mock_crawl_id = 1234

        # inspect only the returned document
        result = [doc for doc in
                  matching.document_merge(match_stream, mock_crawl_id)]

        expected = [
            # merged
            {"url": "b", 'url_hash': 'b', "id": 2,
             "previous": {"url": "a", "id": 1, 'url_hash': 'a'},
             "previous_exists": True},

            # disappeared
            # `crawl_id` and `_id` need to be corrected in this case
            {'id': 3, 'url': 'c', 'crawl_id': 1234, '_id': '1234:3',
             'disappeared': True},

            # new discovered
            {'id': 4, 'url': 'd', 'url_hash': 'd'}
        ]
        self.assertEqual(result, expected)

    def test_document_merge_with_diff(self):
        diff_doc = {'a': {'b': 'changed'}}
        match_stream = iter([
            (MatchingState.MATCH, (self.document1, self.document2, diff_doc)),
        ])
        mock_crawl_id = 1234
        # inspect only the returned document
        result = [doc for doc in
                  matching.document_merge(match_stream, mock_crawl_id)]

        expected = [
            {"url": "b", 'url_hash': 'b', "id": 2,
             "diff": diff_doc,
             "previous": {"url": "a", "id": 1, 'url_hash': 'a'},
             "previous_exists": True}
        ]
        self.assertEqual(result, expected)

    # TODO also need to mock DB
    # TODO mock stream_s3, load_db functions maybe a better idea
    @mock_s3
    def test_document_matching_task(self):
        # prepare mocked s3
        s3 = boto.connect_s3()
        bucket = s3.create_bucket('test_bucket')
        s3_uri = 's3://test_bucket'
        doc_path = 'documents'
        doc_pattern = 'url_documents.json.{}.gz'
        files_json = '{"max_uid_we_crawled": 5}'
        tmp_dir = tempfile.mkdtemp()
        docs = [self.document1, self.document2, self.document3,
                self.document4, self.document5]
        gzip_files = []

        for i, doc in enumerate(docs):
            f = gzip.open(
                os.path.join(tmp_dir, doc_pattern.format(i)), 'w')
            f.write(json.dumps(doc))
            f.close()
            gzip_files.append(f.name)

        # fake document datasets
        key = Key(bucket, name='files.json')
        key.set_contents_from_string(files_json)
        for i, doc in enumerate(docs):
            key = Key(bucket, name=os.path.join(
                doc_path, doc_pattern.format(i)))
            key.set_contents_from_filename(gzip_files[i])

        # test
        match_documents(s3_uri, s3_uri, new_crawl_id=1234,
                        tmp_dir=tmp_dir, part_size=2)
        matched = list_files('s3://test_bucket/documents/comparison')

        self.assertEqual(len(matched), 3)

        shutil.rmtree(tmp_dir)