from __future__ import annotations

import unittest

from simpleflow.swf.mapper.responses import Response


class TestResponse(unittest.TestCase):
    def test_response(self):
        response = Response(foo="bar")
        assert response.foo == "bar"
