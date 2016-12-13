import json
from mock import patch
import unittest

from sure import expect

from simpleflow.swf.helpers import swf_identity


@patch("socket.gethostname")
@patch("getpass.getuser")
@patch("os.getpid")
class TestSwfHelpers(unittest.TestCase):
    def test_swf_identity_standard_case(self, mock_pid, mock_user, mock_host):
        mock_host.return_value = "foo.example.com"
        mock_user.return_value = "root"
        mock_pid.return_value = 1234

        expect(
            json.loads(swf_identity())
        ).to.equal({
            "hostname": "foo.example.com",
            "user": "root",
            "pid": 1234,
        })

    def test_swf_identity_truncated(self, mock_pid, mock_user, mock_host):
        """
        The result should be truncated to 256 characters. Producing an invalid
        JSON string is better than producing an invalid SWF response (for now).
        Later we might externalize this another way (think Data Converters).
        """
        mock_host.return_value = "a" * 250
        mock_user.return_value = "root"
        mock_pid.return_value = 1234

        expect(swf_identity()).to.have.length_of(256)
