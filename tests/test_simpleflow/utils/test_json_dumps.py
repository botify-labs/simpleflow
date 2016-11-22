import datetime
import unittest

import pytz
from simpleflow.utils import json_dumps


class TestJsonDumps(unittest.TestCase):
    def test_json_dumps_basics(self):
        d = datetime.datetime(1970, 1, 1, tzinfo=pytz.UTC)
        cases = [
            [None,         'null'],
            [1,            '1'],
            ["a",          '"a"'],
            [[1, 2],       '[1,2]'],
            [(1, 2),       '[1,2]'],
            [{'a': 'b'},   '{"a":"b"}'],
            [{'start': d}, '{"start":"1970-01-01T00:00:00+00:00"}'],
        ]
        for case in cases:
            self.assertEquals(
                json_dumps(case[0]),
                case[1],
            )

    def test_json_dumps_pretty(self):
        self.assertEquals(
            json_dumps({"z": 1, "abc": "def"}, pretty=True),
            '{\n    "abc": "def",\n    "z": 1\n}',
        )

    def test_json_non_compact(self):
        cases = [
            [None,       'null'],
            [1,          '1'],
            ["a",        '"a"'],
            [[1, 2],     '[1, 2]'],
            [(1, 2),     '[1, 2]'],
            [{'a': 'b'}, '{"a": "b"}'],
        ]
        for case in cases:
            self.assertEquals(
                json_dumps(case[0], compact=False),
                case[1],
            )

    def test_bugfix_154_default(self):
        actual = json_dumps(datetime.datetime(1970, 1, 1), default=lambda _: 'foo')
        expected = '"foo"'
        self.assertEqual(expected, actual)

    def test_default(self):
        actual = json_dumps(datetime.datetime(1970, 1, 1, tzinfo=pytz.UTC))
        expected = '"1970-01-01T00:00:00+00:00"'
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
