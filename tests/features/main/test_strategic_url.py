import shutil
import tempfile
import unittest
from moto import mock_s3
import boto
from cdf.analysis.urls.generators.documents import UrlDocumentGenerator
from cdf.core.streams.base import Stream
from cdf.features.links.streams import OutlinksStreamDef
from cdf.features.main.strategic_url import (
    generate_strategic_stream,
    is_strategic_url,
)
from cdf.features.main.tasks import (
    compute_strategic_urls
)
from cdf.features.main.streams import (
    StrategicUrlStreamDef,
    InfosStreamDef,
    IdStreamDef)
from cdf.features.main.reasons import *


class TestStrategicUrlDetection(unittest.TestCase):
    def setUp(self):
        self.url_id = 1
        self.strategic_http_code = 200
        self.strategic_content_type = 'text/html'
        self.strategic_mask = 0

    def test_noindex(self):
        noindex_mask = 4
        result = is_strategic_url(
            self.url_id,
            noindex_mask,
            self.strategic_http_code,
            self.strategic_content_type
        )
        expected = (False, REASON_NOINDEX.code)
        self.assertEqual(result, expected)

    def test_http_code(self):
        bad_http_code = 301
        result = is_strategic_url(
            self.url_id,
            self.strategic_mask,
            bad_http_code,
            self.strategic_content_type
        )
        expected = (False, REASON_HTTP_CODE.code)
        self.assertEqual(result, expected)

    def test_content_type(self):
        bad_content_type = 'hey/yo'
        result = is_strategic_url(
            self.url_id,
            self.strategic_mask,
            self.strategic_http_code,
            bad_content_type
        )
        expected = (False, REASON_CONTENT_TYPE.code)
        self.assertEqual(result, expected)

    def test_no_canonical(self):
        # no canonical -> strategic
        result = is_strategic_url(
            self.url_id,
            self.strategic_mask,
            self.strategic_http_code,
            self.strategic_content_type
        )
        expected = (True, 0)
        self.assertEqual(result, expected)

    def test_self_canonical(self):
        result = is_strategic_url(
            self.url_id,
            16,
            self.strategic_http_code,
            self.strategic_content_type
        )
        expected = (True, 0)
        self.assertEqual(result, expected)

    def test_canonical(self):
        result = is_strategic_url(
            self.url_id,
            32,
            self.strategic_http_code,
            self.strategic_content_type
        )
        expected = (False, REASON_CANONICAL.code)
        self.assertEqual(result, expected)

    def test_multiple_reasons(self):
        noindex_mask = 4 | 32
        result = is_strategic_url(
            self.url_id,
            noindex_mask,
            self.strategic_http_code,
            self.strategic_content_type
        )
        expected_mask = encode_reason_mask(REASON_CANONICAL, REASON_NOINDEX)
        expected = (False, expected_mask)

        self.assertEqual(result, expected)

    def test_harness(self):
        result = is_strategic_url(
            self.url_id,
            self.strategic_mask,
            self.strategic_http_code,
            self.strategic_content_type
        )
        expected = (True, 0)
        self.assertEqual(result, expected)


TEST_REASON_A = Reason('test_a', 4)
TEST_REASON_B = Reason('test_b', 32)
TEST_REASONS = [TEST_REASON_A, TEST_REASON_B]
decode_func = lambda mask: decode_reason_mask(
    mask, all_reasons=TEST_REASONS)


class TestNonStrategyReason(unittest.TestCase):
    def test_encoding_harness(self):
        mask = encode_reason_mask(TEST_REASON_A)

        # should encode this bit
        result = (mask & TEST_REASON_A.code) == TEST_REASON_A.code
        self.assertTrue(result)

        # should encode ONLY this bit
        result = mask ^ TEST_REASON_A.code
        self.assertEqual(result, 0)

    def test_encoding_multiple(self):
        mask = encode_reason_mask(TEST_REASON_A, TEST_REASON_B)

        # should encode both bit
        result = (mask & TEST_REASON_A.code) == TEST_REASON_A.code
        self.assertTrue(result)
        result = (mask & TEST_REASON_B.code) == TEST_REASON_B.code
        self.assertTrue(result)

    def test_decoding(self):
        nothing = 1
        reasons = decode_func(nothing)
        self.assertEqual(reasons, [])

        # reason A
        mask_a = 4
        reasons = decode_func(mask_a)
        self.assertItemsEqual(reasons, [TEST_REASON_A])

        # reason A and reason B
        mask_b = 37
        reasons = decode_func(mask_b)
        self.assertItemsEqual(reasons, [TEST_REASON_B, TEST_REASON_A])


class TestStrategicUrlStream(unittest.TestCase):
    def setUp(self):
        self.infos_stream = [
            # strategic
            [1, 0, 'text/html', None, None, 200] + [None] * 4,
            # bad content type
            [2, 0, 'yo/yo', None, None, 200] + [None] * 4,
            # no-index
            [3, 4, 'text/html', None, None, 200] + [None] * 4,
            [4, 32, 'text/html', None, None, 200] + [None] * 4,
        ]
        self.outlinks_stream = [
            [4, 'canonical', None, 1, None]
        ]

    def test_harness(self):
        result = list(generate_strategic_stream(
            iter(self.infos_stream),
            iter(self.outlinks_stream))
        )
        expected = [
            (1, True, 0),
            (2, False, REASON_CONTENT_TYPE.code),
            (3, False, REASON_NOINDEX.code),
            (4, False, REASON_CANONICAL.code),
        ]
        self.assertEqual(result, expected)


class TestStrategicUrlTask(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.infos_stream = [
            # strategic
            [1, 0, 'text/html', 0, 0, 200, 0, 0, 0, 'en'],
            # bad content type
            [2, 0, 'yo/yo', 0, 0, 200, 0, 0, 0, 'en'],
            # no-index
            [3, 4, 'text/html', 0, 0, 200, 0, 0, 0, 'en'],
            [4, 32, 'text/html', 0, 0, 200, 0, 0, 0, 'en'],
        ]
        self.outlinks_stream = [
            # canonical to 1
            [4, 'canonical', 0, 1, '']
        ]

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    @mock_s3
    def test_harness(self):
        s3 = boto.connect_s3()
        bucket = s3.create_bucket('test_bucket')
        s3_uri = 's3://test_bucket'
        part_id = 19

        InfosStreamDef.persist(
            self.infos_stream, s3_uri, part_id)
        OutlinksStreamDef.persist(
            self.outlinks_stream, s3_uri, part_id)

        # launch task
        compute_strategic_urls(123, s3_uri,
                               part_id, tmp_dir=self.tmp_dir)

        expected = [
            [1, True, 0],
            [2, False, REASON_CONTENT_TYPE.code],
            [3, False, REASON_NOINDEX.code],
            [4, False, REASON_CANONICAL.code],
        ]
        result = list(StrategicUrlStreamDef.load(
            s3_uri, self.tmp_dir, part_id
        ))

        self.assertEqual(result, expected)


# TODO(darkjh) make a `DocumentGenerationTestCase` that has
#   - pre-defined streams
#   - logic for extracting documents from result stream
class TestDocumentGeneration(unittest.TestCase):
    def setUp(self):
        self.ids = [
            [1, 'http', 'www.site.com', '/path/name.html', ''],
            [2, 'http', 'www.site.com', '/path/name.html', ''],
            [3, 'http', 'www.site.com', '/path/name.html', ''],
        ]
        self.strategy_url = [
            [1, True, 0],  # ok
            [2, False, 4],  # one reason
            [3, False, 5],  # two reasons
        ]

    def test_harness(self):
        ids_stream = Stream(IdStreamDef(), iter(self.ids))
        strategy_url_stream = Stream(StrategicUrlStreamDef(), iter(self.strategy_url))

        documents = list(UrlDocumentGenerator([
            ids_stream,
            strategy_url_stream
        ]))

        # check document 0
        document = documents[0][1]
        self.assertTrue(document['strategic']['is_strategic'])

        # check document 1
        document = documents[1][1]
        self.assertFalse(document['strategic']['is_strategic'])
        reason = document['strategic']['reason']
        self.assertTrue(reason['content_type'])

        # check document 2
        document = documents[2][1]
        self.assertFalse(document['strategic']['is_strategic'])
        reason = document['strategic']['reason']
        self.assertTrue(reason['content_type'])
        self.assertTrue(reason['http_code'])
