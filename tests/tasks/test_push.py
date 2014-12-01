import unittest
import mock

from cdf.exceptions import ErrorRateLimitExceeded
from cdf.tasks.url_data import push_document_stream


def _get_mock_bulk(oks, errors):
    def _mock(*args, **kwargs):
        return oks, errors
    return _mock

# max 5% error rate in this test
MAX_ERROR_RATE = 0.05


class TestDocumentPushError(unittest.TestCase):
    def setUp(self):
        self.stream = iter(['doc1', 'doc2', 'doc3'])

    @mock.patch('cdf.tasks.url_data.bulk', _get_mock_bulk(1000, 0))
    def test_push_all_ok(self):
        # should terminate normally
        push_document_stream(self.stream, None, "", "",
                             max_error_rate=MAX_ERROR_RATE)

    @mock.patch('cdf.tasks.url_data.bulk', _get_mock_bulk(1000, 1))
    def test_push_accept(self):
        # should terminate normally
        push_document_stream(self.stream, None, "", "",
                             max_error_rate=MAX_ERROR_RATE)

    @mock.patch('cdf.tasks.url_data.bulk', _get_mock_bulk(94, 6))
    def test_push_not_accept(self):
        # should raise exception
        self.assertRaises(
            ErrorRateLimitExceeded,
            push_document_stream,
            self.stream,  # mock stream
            None, "", "",  # not important param
            MAX_ERROR_RATE
        )