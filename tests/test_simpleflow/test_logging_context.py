from __future__ import annotations

import unittest

from sure import expect

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
        expect(ctx.get("workflow_id")).to.equal("")
        expect(ctx.get("event_id")).to.equal("")

        ctx.set("workflow_id", "foo-bar")
        ctx.set("event_id", 4)
        expect(ctx.get("workflow_id")).to.equal("foo-bar")
        expect(ctx.get("event_id")).to.equal("4")

    def test_set_and_get_invalid_key(self):
        (expect(ctx.set).when.called_with("invalid_key", "bar").to.have.raised(KeyError))

        (expect(ctx.get).when.called_with("invalid_key").to.have.raised(KeyError))

    def test_reset(self):
        ctx.set("workflow_id", "foo-bar")
        ctx.set("task_list", "tl")
        expect(ctx.get("workflow_id")).to.equal("foo-bar")
        expect(ctx.get("task_list")).to.equal("tl")

        ctx.reset()
        expect(ctx.get("workflow_id")).to.equal("")
        expect(ctx.get("task_list")).to.equal("")
