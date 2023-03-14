from __future__ import annotations

import unittest

from sure import expect

from simpleflow.utils import format_exc, to_k8s_identifier


class MyTestCase(unittest.TestCase):
    def test_format_final_exc_line(self):
        line = None
        try:
            {}[1]
        except Exception as e:
            line = format_exc(e)
        self.assertEqual("KeyError: 1", line)

    def test_to_k8s_identifier(self):
        cases = [
            ["mod.ule.foo_bar_baz", "mod-ule-foo-bar-baz"],
            ["double_____dash", "double-dash"],
            ["FooBar", "foobar"],
        ]
        for case in cases:
            expect(to_k8s_identifier(case[0])).to.equal(case[1])


if __name__ == "__main__":
    unittest.main()
