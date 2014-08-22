import shutil
import tempfile
import unittest
from moto import mock_s3
import boto
from cdf.features.links.streams import OutlinksStreamDef
from cdf.features.main.strategic_url import (
    generate_strategic_stream,
    is_strategic_url,
    compute_strategic_urls
)
from cdf.features.main.reasons import (
    Reason,
    decode_reason_mask,
    encode_reason_mask
)
from cdf.features.main.streams import (
    StrategicUrlStreamDef, InfosStreamDef
)


class TestStrategicUrlDetection(unittest.TestCase):
    def setUp(self):
        self.url_id = 1
        self.strategic_http_code = 200
        self.strategic_content_type = 'text/html'
        self.strategic_mask = 0
        self.strategic_outlinks = []

    def test_noindex(self):
        noindex_mask = 4
        result = is_strategic_url(
            self.url_id,
            noindex_mask,
            self.strategic_http_code,
            self.strategic_content_type,
            self.strategic_outlinks
        )
        self.assertFalse(result)

    def test_http_code(self):
        bad_http_code = 301
        result = is_strategic_url(
            self.url_id,
            self.strategic_mask,
            bad_http_code,
            self.strategic_content_type,
            self.strategic_outlinks
        )
        self.assertFalse(result)

    def test_content_type(self):
        bad_content_type = 'hey/yo'
        result = is_strategic_url(
            self.url_id,
            self.strategic_mask,
            self.strategic_http_code,
            bad_content_type,
            self.strategic_outlinks
        )
        self.assertFalse(result)

    def test_no_canonical(self):
        # no canonical -> strategic
        outlinks = [
            [1, 'a', None, 4, '']
        ]
        result = is_strategic_url(
            self.url_id,
            self.strategic_mask,
            self.strategic_http_code,
            self.strategic_content_type,
            outlinks
        )
        self.assertTrue(result)

    def test_self_canonical(self):
        # self canonical -> strategic
        outlinks = [
            [1, 'canonical', None, 1, '']
        ]
        result = is_strategic_url(
            self.url_id,
            self.strategic_mask,
            self.strategic_http_code,
            self.strategic_content_type,
            outlinks
        )
        self.assertTrue(result)

    def test_canonical(self):
        # has canonical to other page -> non-strategic
        outlinks = [
            [1, 'canonical', None, 4, '']
        ]
        result = is_strategic_url(
            self.url_id,
            self.strategic_mask,
            self.strategic_http_code,
            self.strategic_content_type,
            outlinks
        )
        self.assertFalse(result)

    def test_first_canonical(self):
        # only take the first canonical into account
        # so even if we have a self-canonical, this url
        # is still non strategic
        outlinks = [
            [1, 'canonical', None, 4, ''],
            [1, 'canonical', None, 1, '']  # self canonical
        ]
        result = is_strategic_url(
            self.url_id,
            self.strategic_mask,
            self.strategic_http_code,
            self.strategic_content_type,
            outlinks
        )
        self.assertFalse(result)

    def test_harness(self):
        result = is_strategic_url(
            self.url_id,
            self.strategic_mask,
            self.strategic_http_code,
            self.strategic_content_type,
            self.strategic_outlinks
        )
        self.assertTrue(result)


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
        self.assertItemsEqual(reasons, ['test_a'])

        # reason A and reason B
        mask_b = 37
        reasons = decode_func(mask_b)
        self.assertItemsEqual(reasons, ['test_b', 'test_a'])


class TestStrategicUrlStream(unittest.TestCase):
    def setUp(self):
        self.infos_stream = [
            # strategic
            [1, 0, 'text/html', None, None, 200] + [None] * 4,
            # bad content type
            [2, 0, 'yo/yo', None, None, 200] + [None] * 4,
            # no-index
            [3, 4, 'text/html', None, None, 200] + [None] * 4,
            [4, 0, 'text/html', None, None, 200] + [None] * 4,
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
            (1, True),
            (2, False),
            (3, False),
            (4, False),
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
            [4, 0, 'text/html', 0, 0, 200, 0, 0, 0, 'en'],
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

        InfosStreamDef.persist_part_to_s3(
            self.infos_stream, s3_uri, part_id)
        OutlinksStreamDef.persist_part_to_s3(
            self.outlinks_stream, s3_uri, part_id)

        # launch task
        compute_strategic_urls(123, s3_uri,
                               part_id, tmp_dir=self.tmp_dir)

        expected = [
            [1, True],
            [2, False],
            [3, False],
            [4, False],
        ]
        result = list(StrategicUrlStreamDef.get_stream_from_s3(
            s3_uri, self.tmp_dir, part_id
        ))

        self.assertEqual(result, expected)