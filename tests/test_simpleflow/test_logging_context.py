from __future__ import annotations

import unittest

import pytest

from simpleflow import logging_context as ctx


class TestProcessContext(unittest.TestCase):
    def tearDown(self):
        # This removes SWF context keys from environment variables.
        # It's a bit weird to use this to ensure things are cleaned
        # up after tests but 1/ this is tested in a specific test
        # and 2/ it's better than messing with this module internal
        # implementation details.
        ctx.reset()

    def test_set_and_get(self):
        assert ctx.get("workflow_id") == ""
        assert ctx.get("event_id") == ""

        ctx.set("workflow_id", "foo-bar")
        ctx.set("event_id", 4)
        assert ctx.get("workflow_id") == "foo-bar"
        assert ctx.get("event_id") == "4"

    def test_set_and_get_invalid_key(self):
        with pytest.raises(KeyError):
            ctx.set("invalid_key", "bar")

        with pytest.raises(KeyError):
            ctx.get("invalid_key")

    def test_reset(self):
        ctx.set("workflow_id", "foo-bar")
        ctx.set("task_list", "tl")
        assert ctx.get("workflow_id") == "foo-bar"
        assert ctx.get("task_list") == "tl"

        ctx.reset()
        assert ctx.get("workflow_id") == ""
        assert ctx.get("task_list") == ""
