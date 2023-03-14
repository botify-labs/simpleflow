from __future__ import annotations

import unittest

from sure import expect

from simpleflow import settings


class TestSettings(unittest.TestCase):
    def test_overriding_a_setting(self):
        expect(settings.METROLOGY_PATH_PREFIX).to.equal(None)

        settings.put_setting("METROLOGY_PATH_PREFIX", "123")
        expect(settings.METROLOGY_PATH_PREFIX).to.equal("123")

    def test_change_multiple_settings(self):
        dct = {"FOO": "foo", "BAR": "bar"}
        settings.configure(dct)

        expect(settings.FOO).to.equal("foo")
        expect(settings.BAR).to.equal("bar")
