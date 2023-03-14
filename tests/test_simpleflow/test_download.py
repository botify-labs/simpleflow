from __future__ import annotations

import os
import shutil
import unittest
from unittest.mock import patch

from sure import expect

from simpleflow.download import RemoteBinary, with_binaries

# example binary remote/local location
remote_location = "s3://a.bucket/v1.2.3/custom-bin"
local_directory = "/tmp/simpleflow-binaries/custom-bin-585b3e5c252d6ec7aff52c24b149d719"
local_location = "/tmp/simpleflow-binaries/custom-bin-585b3e5c252d6ec7aff52c24b149d719/custom-bin"


def fake_download_binary(binary):
    os.makedirs(binary.local_directory)
    with open(binary.local_location, "a"):
        os.utime(binary.local_location, None)  # aka "touch"
        os.chmod(binary.local_location, 0o755)


class TestRemoteBinary(unittest.TestCase):
    def setUp(self):
        self._cleanup()

    def tearDown(self):
        self._cleanup()

    def _cleanup(self):
        shutil.rmtree(local_directory, ignore_errors=True)

    def test_locations_computing(self):
        binary = RemoteBinary("custom-bin", remote_location)
        expect(binary.local_directory).to.equal(local_directory)
        expect(binary.local_location).to.equal(local_location)

    @patch("simpleflow.download.RemoteBinary._download_binary")
    def test_should_do_nothing_if_binary_present(self, method_mock):
        binary = RemoteBinary("custom-bin", remote_location)
        fake_download_binary(binary)

        # binary already here and executable, we should *not* call _download_binary
        binary.download()
        expect(method_mock.call_count).to.equal(0)

    @patch("simpleflow.download.RemoteBinary._download_binary", return_value=None)
    def test_should_download_the_binary_if_needed(self, method_mock):
        binary = RemoteBinary("custom-bin", remote_location)

        # binary is not here, we should call _download_binary
        binary.download()
        method_mock.assert_called_once_with()


class TestWithBinariesDecorator(unittest.TestCase):
    @with_binaries({"custom-bin": remote_location})
    def method_needing_custom_binary(self):
        return "foo!"

    @patch("simpleflow.download.RemoteBinary._download_binary", return_value=None)
    def test_with_binaries_decorator(self, method_mock):
        res = self.method_needing_custom_binary()
        expect(res).to.equal("foo!")

        method_mock.assert_called_once_with()
