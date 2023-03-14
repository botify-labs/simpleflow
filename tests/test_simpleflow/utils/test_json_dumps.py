from __future__ import annotations

import datetime
import json
import unittest

import pytz

from simpleflow.exceptions import ExecutionBlocked
from simpleflow.futures import Future
from simpleflow.utils import json_dumps


class TestJsonDumps(unittest.TestCase):
    def test_json_dumps_basics(self):
        d = datetime.datetime(1970, 1, 1, tzinfo=pytz.UTC)
        cases = [
            [None, "null"],
            [1, "1"],
            ["a", '"a"'],
            [[1, 2], "[1,2]"],
            [(1, 2), "[1,2]"],
            [{"a": "b"}, '{"a":"b"}'],
            [{"start": d}, '{"start":"1970-01-01T00:00:00Z"}'],
        ]
        for case in cases:
            self.assertEqual(
                json_dumps(case[0]),
                case[1],
            )

    def test_json_dumps_futures(self):
        resolved = Future()
        resolved.set_finished("foo")
        self.assertEqual(json_dumps(resolved), '"foo"')

        pending = Future()
        with self.assertRaises(ExecutionBlocked):
            json_dumps(pending)

    def test_json_dumps_pretty(self):
        self.assertEqual(
            json_dumps({"z": 1, "abc": "def"}, pretty=True),
            '{\n    "abc": "def",\n    "z": 1\n}',
        )

    def test_json_non_compact(self):
        cases = [
            [None, "null"],
            [1, "1"],
            ["a", '"a"'],
            [[1, 2], "[1, 2]"],
            [(1, 2), "[1, 2]"],
            [{"a": "b"}, '{"a": "b"}'],
        ]
        for case in cases:
            self.assertEqual(
                json_dumps(case[0], compact=False),
                case[1],
            )

    def test_bugfix_154_default(self):
        actual = json_dumps(datetime.datetime(1970, 1, 1), default=lambda _: "foo")
        expected = '"foo"'
        self.assertEqual(expected, actual)

    def test_default(self):
        actual = json_dumps(datetime.datetime(1970, 1, 1, tzinfo=pytz.UTC))
        expected = '"1970-01-01T00:00:00Z"'
        self.assertEqual(expected, actual)

    def test_proxy(self):
        from lazy_object_proxy import Proxy

        def unwrap():
            return "foo"

        data = {"args": [Proxy(unwrap)]}
        expected = '{"args":["foo"]}'
        actual = json_dumps(data)
        self.assertEqual(expected, actual)

    def test_proxy_dict(self):
        from lazy_object_proxy import Proxy

        def unwrap():
            return {"foo": 3}

        data = {"args": [Proxy(unwrap)]}
        expected = '{"args":[{"foo":3}]}'
        actual = json_dumps(data)
        self.assertEqual(expected, actual)

    def test_set(self):
        data = [
            {1, 2, 3},
            frozenset([-1, -2, -3]),
        ]
        expected = [
            [1, 2, 3],
            [-1, -2, -3],
        ]
        actual = json_dumps(data)
        actual = json.loads(actual)
        self.assertEqual(sorted(expected[0]), sorted(actual[0]))
        self.assertEqual(sorted(expected[1]), sorted(actual[1]))


if __name__ == "__main__":
    unittest.main()
