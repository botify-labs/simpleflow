from __future__ import annotations

import unittest

from sure import expect

from simpleflow.log import SimpleflowFormatter


class FakeRecord:
    def __init__(self, msg, args):
        self.msg = msg
        self.args = args
        self.created = 0.0
        self.levelname = "INFO"
        self.processName = "foo"
        self.process = 123
        self.exc_info = None
        self.exc_text = None


class TestLog(unittest.TestCase):
    def test_interpolation_doesnt_break_needlessly(self):
        formatter = SimpleflowFormatter()
        record = FakeRecord("Foo %s", [])

        expect(formatter.format(record)).to.match(r"Foo %s$")
