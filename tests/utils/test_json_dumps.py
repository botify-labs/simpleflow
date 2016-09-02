import unittest
from datetime import datetime

from simpleflow.utils import json_dumps


class TestJsonDumps(unittest.TestCase):
    def test_json_dumps_basics(self):
        cases = [
            [ None,       'null'],
            [ 1,          '1'],
            [ "a",        '"a"'],
            [ [1, 2],     '[1, 2]'],
            [ (1, 2),     '[1, 2]'],
            [ {'a': 'b'}, '{"a": "b"}'],
        ]
        for case in cases:
            self.assertEquals(
                json_dumps(case[0]),
                case[1],
            )

    def test_json_dumps_pretty(self):
        self.assertEquals(
                json_dumps({"abc": "def"}, pretty=True),
                '{\n    "abc": "def"\n}',
        )
