from __future__ import annotations

import unittest

from swf.responses import Response


class TestResponse(unittest.TestCase):
    def test_response(self):
        response = Response(foo="bar")
        assert response.foo == "bar"
