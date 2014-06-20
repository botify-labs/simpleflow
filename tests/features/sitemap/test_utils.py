import unittest
import mock

from cdf.features.sitemap.utils import download_url
from cdf.features.sitemap.exceptions import DownloadError


class TestDownloadUrl(unittest.TestCase):
    @mock.patch("time.sleep", new=mock.MagicMock())
    @mock.patch("cdf.features.sitemap.utils.requests")
    def test_repeated_download_error(self, request_mock):
        response_mock = mock.MagicMock
        response_mock.status_code = 404
        request_mock.get.return_value = response_mock
        self.assertRaises(
            DownloadError,
            download_url,
            "foo",
            "/tmp/foo")
