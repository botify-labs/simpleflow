from __future__ import unicode_literals

import unittest

import subprocess

from mock import patch

from simpleflow.compat import unicode
from simpleflow.utils.subprocess_utils import communicate_with_limits_naive, communicate_with_limits_poll, mswindows, \
    communicate_with_limits


class Tests(object):
    # FIXME need more tests

    communicate_with_limits = None

    def test_is_dead(self):
        proc = subprocess.Popen(['/bin/cat'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        proc.stdin.write(b'abc\ndef\nghi\n')
        self.communicate_with_limits(proc, None, 'utf-8', 'utf-8', 4, None)
        self.assertEqual(0, proc.returncode)

    def test_is_unicode(self):
        proc = subprocess.Popen(['/bin/cat'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None)
        proc.stdin.write(b'abc\ndef\nghi\n')
        stdout, _ = self.communicate_with_limits(proc, None, 'utf-8', 'utf-8', 4, None)
        self.assertIsInstance(stdout, unicode)

    def test_limit_1(self):
        proc = subprocess.Popen(['/bin/cat'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        proc.stdin.write(b'abc\ndef\nghi\n')
        stdout, stderr = self.communicate_with_limits(proc, None, 'utf-8', 'utf-8', 4, None)
        expected = 'ghi\n'
        self.assertEqual(expected, stdout)
        self.assertEqual('', stderr)


class TestCommunicateNaive(unittest.TestCase, Tests):
    def setUp(self):
        self.communicate_with_limits = communicate_with_limits_naive


@unittest.skipIf(mswindows or not subprocess._has_poll,
                 'Not implemented on Windows ({}) or without select.poll ({}), '.format(
                     mswindows, not subprocess._has_poll)
                 )
class TestCommunicateWithPoll(unittest.TestCase, Tests):
    def setUp(self):
        self.communicate_with_limits = communicate_with_limits_poll

    def test_right_method_called(self):
        with patch('simpleflow.utils.subprocess_utils.communicate_with_limits_poll') as \
                mock_communicate_with_limits_poll:
            mock_communicate_with_limits_poll.return_value = None, None
            proc = subprocess.Popen(['/bin/cat'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            communicate_with_limits(proc, None, 'utf-8', 'utf-8', 4, None)
            mock_communicate_with_limits_poll.assert_called_once()
            proc.terminate()
            proc.wait()
