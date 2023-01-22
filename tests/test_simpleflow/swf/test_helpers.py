from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from sure import expect

from simpleflow.swf.helpers import swf_identity


@patch("socket.gethostname")
@patch("getpass.getuser")
@patch("os.getpid")
@patch("psutil.Process")
class TestSwfHelpers(unittest.TestCase):
    def test_swf_identity_standard_case(self, mock_process, mock_pid, mock_user, mock_host):
        mock_host.return_value = "foo.example.com"
        mock_user.return_value = "root"
        mock_pid.return_value = 1234
        mock_process.return_value.exe.return_value = "/bin/python8"

        expect(json.loads(swf_identity())).to.equal(
            {
                "hostname": "foo.example.com",
                "user": "root",
                "pid": 1234,
                "exe": "/bin/python8",
            }
        )

    def test_swf_identity_with_extra_environment(self, mock_process, mock_pid, mock_user, mock_host):
        """
        SIMPLEFLOW_IDENTITY environment variable can provide extra keys.
        """
        mock_host.return_value = "foo.example.com"
        mock_user.return_value = "root"
        mock_pid.return_value = 1234
        mock_process.return_value.exe.return_value = "/bin/python8"

        with patch.dict(
            "os.environ",
            {"SIMPLEFLOW_IDENTITY": '{"version":"1.2.3","hostname":"bar.example.com"}'},
        ):
            identity = json.loads(swf_identity())

        expect(identity["hostname"]).to.equal("bar.example.com")
        expect(identity).to.have.key("version").being.equal("1.2.3")

    def test_swf_identity_with_invalid_extra_environment(self, mock_process, mock_pid, mock_user, mock_host):
        """
        If SIMPLEFLOW_IDENTITY is invalid, it should just be ignored.
        """
        mock_host.return_value = "foo.example.com"
        mock_user.return_value = "root"
        mock_pid.return_value = 1234
        mock_process.return_value.exe.return_value = "/bin/python8"

        with patch.dict("os.environ", {"SIMPLEFLOW_IDENTITY": "not a json string"}):
            identity = json.loads(swf_identity())

        expect(identity["hostname"]).to.equal("foo.example.com")
        expect(identity).to_not.have.key("version")

    def test_swf_identity_with_null_values_in_environment(self, mock_process, mock_pid, mock_user, mock_host):
        """
        SIMPLEFLOW_IDENTITY null values can remove default keys.
        """
        mock_host.return_value = "foo.example.com"
        mock_user.return_value = "root"
        mock_pid.return_value = 1234
        mock_process.return_value.exe.return_value = "/bin/python8"

        with patch.dict("os.environ", {"SIMPLEFLOW_IDENTITY": '{"foo":null,"user":null}'}):
            identity = json.loads(swf_identity())

        # key removed
        expect(identity).to_not.have.key("user")
        # key ignored
        expect(identity).to_not.have.key("foo")
