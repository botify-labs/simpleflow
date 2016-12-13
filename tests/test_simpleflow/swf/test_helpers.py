import json
from mock import patch
import os
import unittest

from sure import expect

from simpleflow.swf.helpers import swf_identity


@patch("socket.gethostname")
@patch("getpass.getuser")
@patch("os.getpid")
class TestSwfHelpers(unittest.TestCase):
    def tearDown(self):
        if "SIMPLEFLOW_IDENTITY" in os.environ:
            del os.environ["SIMPLEFLOW_IDENTITY"]

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

    def test_swf_identity_with_extra_environment(self, mock_pid, mock_user, mock_host):
        """
        SIMPLEFLOW_IDENTITY environment variable can provide extra keys.
        """
        mock_host.return_value = "foo.example.com"
        mock_user.return_value = "root"
        mock_pid.return_value = 1234
        os.environ["SIMPLEFLOW_IDENTITY"] = '{"version":"1.2.3","hostname":"bar.example.com"}'

        identity = json.loads(swf_identity())

        expect(identity["hostname"]).to.equal("bar.example.com")
        expect(identity).to.have.key("version").being.equal("1.2.3")

    def test_swf_identity_with_invalid_extra_environment(self, mock_pid, mock_user, mock_host):
        """
        If SIMPLEFLOW_IDENTITY is invalid, it should just be ignored.
        """
        mock_host.return_value = "foo.example.com"
        mock_user.return_value = "root"
        mock_pid.return_value = 1234
        os.environ["SIMPLEFLOW_IDENTITY"] = 'not a json string'

        identity = json.loads(swf_identity())

        expect(identity["hostname"]).to.equal("foo.example.com")
        expect(identity).to_not.have.key("version")

    def test_swf_identity_with_null_values_in_environment(self, mock_pid, mock_user, mock_host):
        """
        SIMPLEFLOW_IDENTITY null values can remove default keys.
        """
        mock_host.return_value = "foo.example.com"
        mock_user.return_value = "root"
        mock_pid.return_value = 1234
        os.environ["SIMPLEFLOW_IDENTITY"] = '{"foo":null,"user":null}'

        identity = json.loads(swf_identity())

        # key removed
        expect(identity).to_not.have.key("user")
        # key ignored
        expect(identity).to_not.have.key("foo")
