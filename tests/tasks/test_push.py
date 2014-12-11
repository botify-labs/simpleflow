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

    @mock.patch('cdf.tasks.url_data.EsHandler')
    def test_push_all_ok(self, es_handler):
        es_handler.raw_bulk_index = _get_mock_bulk(1000, 0)
        # should terminate normally
        push_document_stream(self.stream, es_handler,
                             max_error_rate=MAX_ERROR_RATE)

    @mock.patch('cdf.tasks.url_data.EsHandler')
    def test_push_accept(self, es_handler):
        es_handler.raw_bulk_index = _get_mock_bulk(1000, 1)
        # should terminate normally
        push_document_stream(self.stream, es_handler,
                             max_error_rate=MAX_ERROR_RATE)

    @mock.patch('cdf.tasks.url_data.EsHandler')
    def test_push_not_accept(self, es_handler):
        es_handler.raw_bulk_index = _get_mock_bulk(94, 6)
        # should raise exception
        self.assertRaises(
            ErrorRateLimitExceeded,
            push_document_stream,
            self.stream,  # mock stream
            es_handler,
            MAX_ERROR_RATE
        )