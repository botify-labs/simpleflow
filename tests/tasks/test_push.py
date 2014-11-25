import unittest
import mock

from cdf.tasks.url_data import push_document_stream


def _get_mock_bulk(oks, errors):
    def _mock(*args, **kwargs):
        return oks, errors
    return _mock


class TestDocumentPushError(unittest.TestCase):
    def setUp(self):
        self.stream = iter(['doc1', 'doc2', 'doc3'])

    @mock.patch('cdf.tasks.url_data.bulk', _get_mock_bulk(1000, 1))
    def test_push_accept(self):
        # should terminate normally
        push_document_stream(self.stream, None, "", "")

    @mock.patch('cdf.tasks.url_data.bulk', _get_mock_bulk(50, 50))
    def test_push_not_accept(self):
        # should raise exception
        self.assertRaises(
            Exception,
            push_document_stream,
            self.stream,  # mock stream
            None, "", ""  # not important param
        )