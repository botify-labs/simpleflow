import unittest
from datetime import datetime

import pytz

from simpleflow.utils import json_dumps


class JsonDumpsTestCase(unittest.TestCase):
    def test_bugfix_154_default(self):
        actual = json_dumps(datetime(1970, 1, 1), default=lambda _: 'foo')
        expected = '"foo"'
        self.assertEqual(expected, actual)

    def test_default(self):
        actual = json_dumps(datetime(1970, 1, 1, tzinfo=pytz.UTC))
        expected = '"1970-01-01T00:00:00+00:00"'
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
