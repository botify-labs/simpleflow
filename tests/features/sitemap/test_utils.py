import unittest
import mock
import requests
from cdf.features.sitemap.utils import download_url
from cdf.features.sitemap.exceptions import DownloadError


class TestDownloadUrl(unittest.TestCase):
    @mock.patch("time.sleep", autospec=True)
    @mock.patch("cdf.features.sitemap.utils.requests", autospec=True)
    def test_repeated_download_error(self, request_mock, sleep_mock):
        response_mock = mock.MagicMock
        response_mock.status_code = 404
        request_mock.get.return_value = response_mock
        self.assertRaises(
            DownloadError,
            download_url,
            "foo",
            "/tmp/foo")
        self.assertEqual(7, request_mock.get.call_count)

    @mock.patch("__builtin__.open", new=mock.mock_open())
    @mock.patch("cdf.features.sitemap.utils.requests", autospec=True)
    def test_user_agent(self, request_mock):
        #mock request answer
        response_mock = mock.create_autospec(requests.Response)
        response_mock.status_code = 200
        response_mock.iter_content.return_value = iter([])
        request_mock.get.return_value = response_mock

        user_agent = "foo bar"
        download_url("http://foo/bar.html",
                     "/tmp/bar.html",
                     user_agent=user_agent)

        request_mock.get.assert_called_once_with("http://foo/bar.html",
                                                 headers={"User-Agent": "foo bar"})
