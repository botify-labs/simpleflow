from __future__ import annotations

import unittest

from simpleflow import settings


class TestSettings(unittest.TestCase):
    def test_overriding_a_setting(self):
        assert settings.METROLOGY_PATH_PREFIX is None

        settings.put_setting("METROLOGY_PATH_PREFIX", "123")
        assert settings.METROLOGY_PATH_PREFIX == "123"

    def test_change_multiple_settings(self):
        dct = {"FOO": "foo", "BAR": "bar"}
        settings.configure(dct)

        assert settings.FOO == "foo"
        assert settings.BAR == "bar"
